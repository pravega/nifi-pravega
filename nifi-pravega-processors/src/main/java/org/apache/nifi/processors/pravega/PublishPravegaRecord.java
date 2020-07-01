/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package org.apache.nifi.processors.pravega;

import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"Pravega", "Nautilus", "Put", "Send", "Publish", "Stream", "Record"})
@CapabilityDescription("Sends the contents of a FlowFile as individual records to Pravega. "
        + "The contents of the FlowFile are expected to be record-oriented data that can be read by the configured Record Reader.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@SeeAlso({PublishPravega.class, ConsumePravega.class})
public class PublishPravegaRecord extends AbstractPravegaPublisher {
    static final List<PropertyDescriptor> descriptors;

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to Pravega")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor ROUTING_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("routing-key-field")
            .displayName("Routing Key Field")
            .description("The name of a field in the Input Records that should be used as the Routing Key for the Pravega event.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(false)
            .build();

    static {
        final List<PropertyDescriptor> innerDescriptorsList = getAbstractPropertyDescriptors();
        innerDescriptorsList.add(RECORD_READER);
        innerDescriptorsList.add(RECORD_WRITER);
        innerDescriptorsList.add(ROUTING_KEY_FIELD);
        descriptors = Collections.unmodifiableList(innerDescriptorsList);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory, final ProcessSession session) throws ProcessException {
        logger.debug("onTrigger: BEGIN");

        final double maxKiBInTransaction = 1024.0;
        final int maxEventsInTransaction = 1000;
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(
                maxKiBInTransaction, DataUnit.KB, maxEventsInTransaction));
        if (flowFiles.isEmpty()) {
            return;
        }

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        final String controller = context.getProperty(PROP_CONTROLLER).getValue();
        final String scope = context.getProperty(PROP_SCOPE).getValue();
        final String streamName = context.getProperty(PROP_STREAM).getValue();
        final String transitUri = buildTransitURI(controller, scope, streamName);
        final long startTime = System.nanoTime();
        final TransactionalEventStreamWriter<byte[]> writer = getWriter(context);
        final Transaction<byte[]> transaction = writer.beginTxn();
        final UUID txnId = transaction.getTxnId();

        logger.info("Sending {} FlowFiles to Pravega stream {} in transaction {}.",
                new Object[]{flowFiles.size(), transitUri, txnId});

        final AtomicLong recordCount = new AtomicLong(0);

        try {
            for (final FlowFile flowFile : flowFiles) {
                if (!isScheduled()) {
                    // If stopped, re-queue FlowFile instead of sending it
                    session.transfer(flowFile);
                    transaction.abort();
                    continue;
                }

                final String routingKeyField = context.getProperty(ROUTING_KEY_FIELD).evaluateAttributeExpressions(flowFile).getValue();

                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream rawIn) throws IOException {
                        try (final InputStream in = new BufferedInputStream(rawIn)) {
                            final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
                            final RecordSet recordSet = reader.createRecordSet();
                            final RecordSchema schema = writerFactory.getSchema(flowFile.getAttributes(), recordSet.getSchema());

                            final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

                            Record record;

                            while ((record = recordSet.next()) != null) {
                                recordCount.incrementAndGet();
                                baos.reset();

                                // Write record to byte array.
                                try (final RecordSetWriter recordSetWriter = writerFactory.createWriter(logger, schema, baos)) {
                                    recordSetWriter.write(record);
                                    recordSetWriter.flush();
                                }

                                final byte[] messageContent = baos.toByteArray();
                                final String routingKey = routingKeyField == null ? "" : record.getAsString(routingKeyField);

                                if (logger.isDebugEnabled()) {
                                    final String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
                                    logger.debug("routingKey={}, size={}, flowFileUUID={}",
                                            new Object[]{routingKey, flowFile.getSize(), flowFileUUID});
                                    logger.trace("messageContent={}", new Object[]{dumpByteArray(messageContent)});
                                }

                                // Write to Pravega.
                                transaction.writeEvent(routingKey, messageContent);
                            }
                        } catch (final SchemaNotFoundException | MalformedRecordException | TxnFailedException e) {
                            logger.error(e.getMessage());
                            throw new ProcessException(e);
                        }
                    }
                });
            }
            // Flush all events to Pravega's durable storage.
            // This will block until done.
            // It will not commit the transaction.
            transaction.flush();
        }
        catch (ProcessException | TxnFailedException e) {
            logger.error(e.getMessage());
            // Transfer the FlowFiles to the failure relationship.
            // The user can choose to route the FlowFiles back to this processor for retry
            // or they can route them to an alternate processor.
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }

        final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        // Transfer the FlowFiles to the success relationship.
        for (FlowFile flowFile : flowFiles) {
            session.getProvenanceReporter().send(flowFile, transitUri, transmissionMillis);
            session.transfer(flowFile, REL_SUCCESS);
        }

        // Now we can commit the Pravega transaction.
        // If an error occurs, we must rollback the NiFi session because we have already transferred
        // FlowFiles to the success relationship. We then transfer the FlowFiles to the failure relationship.
        try {
            transaction.commit();
        } catch (TxnFailedException e) {
            logger.error(e.getMessage());
            session.rollback();
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }

        // Commit the NiFi session so that the following log message indicates complete success.
        session.commit();

        logger.info("Sent {} records (events) in {} FlowFiles in {} milliseconds to Pravega stream {} in transaction {}.",
                new Object[]{recordCount.get(), flowFiles.size(), transmissionMillis, transitUri, txnId});
    }

}
