package org.apache.nifi.processors.pravega;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Tags({"Pravega", "Nautilus", "Put", "Send", "Publish", "Stream"})
@CapabilityDescription("Sends the contents of a FlowFile as an event to Pravega.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@ReadsAttributes({@ReadsAttribute(attribute=PublishPravega.ATTR_ROUTING_KEY, description="The Pravega routing key")})
@SeeAlso({ConsumePravega.class})
public class PublishPravega extends AbstractPravegaPublisher {
    static final String ATTR_ROUTING_KEY = "pravega.routing.key";

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        logger.debug("PublishPravega.onTrigger: BEGIN");

        final double maxKiBInTransaction = 1024.0;
        final int maxEventsInTransaction = 1000;
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(
                maxKiBInTransaction, DataUnit.KB, maxEventsInTransaction));
        if (flowFiles.isEmpty()) {
            return;
        }

        final String controller = context.getProperty(PROP_CONTROLLER).getValue();
        final String scope = context.getProperty(PROP_SCOPE).getValue();
        final String streamName = context.getProperty(PROP_STREAM).getValue();
        final String transitUri = buildTransitURI(controller, scope, streamName);
        final long startTime = System.nanoTime();
        final EventStreamWriter<byte[]> writer = getWriter(context);
        final Transaction<byte[]> transaction = writer.beginTxn();
        final UUID txnId = transaction.getTxnId();

        logger.info("Sending {} messages to Pravega stream {} in transaction {}.",
                new Object[]{flowFiles.size(), transitUri, txnId});

        try {
            for (final FlowFile flowFile : flowFiles) {
                if (!isScheduled()) {
                    // If stopped, re-queue FlowFile instead of sending it
                    session.transfer(flowFile);
                    transaction.abort();
                    continue;
                }

                String routingKey = flowFile.getAttribute(ATTR_ROUTING_KEY);
                if (routingKey == null) {
                    routingKey = "";
                }

                // Read FlowFile contents.
                final byte[] messageContent = new byte[(int) flowFile.getSize()];
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        StreamUtils.fillBuffer(in, messageContent, true);
                    }
                });

                if (logger.isDebugEnabled()) {
                    final String flowFileUUID = flowFile.getAttribute(CoreAttributes.UUID.key());
                    logger.debug("routingKey={}, size={}, flowFileUUID={}",
                            new Object[]{routingKey, flowFile.getSize(), flowFileUUID});
                    logger.debug("messageContentStr={}, messageContent={}", new Object[]{
                            new String(messageContent, StandardCharsets.UTF_8), messageContent});
                }

                // Write to Pravega.
                transaction.writeEvent(routingKey, messageContent);
            }
            // Flush all events to Pravega's durable storage.
            // This will block until done.
            // It will not commit the transaction.
            transaction.flush();
        }
        catch (TxnFailedException e) {
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

        logger.info("Sent {} messages in {} milliseconds to Pravega stream {} in transaction {}.",
                new Object[]{flowFiles.size(), transmissionMillis, transitUri, txnId});
    }

}
