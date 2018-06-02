/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pravega;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Tags({"Pravega", "Nautilus", "Put", "Send", "Publish"})
@CapabilityDescription("Sends the contents of a FlowFile as an event to Pravega.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@ReadsAttributes({@ReadsAttribute(attribute=PublishPravega.ATTR_ROUTING_KEY, description="The Pravega routing key")})
public class PublishPravega extends AbstractProcessor {
    static final String ATTR_ROUTING_KEY = "pravega.routing.key";

    static final Validator CONTROLLER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            try{
                final URI controllerURI = new URI(input);
            } catch (URISyntaxException e) {
                return new ValidationResult.Builder().subject(subject).valid(false).explanation("it is not valid URI syntax.").build();
            }
            return new ValidationResult.Builder().subject(subject).valid(true).build();
        }
    };

    static final PropertyDescriptor PROP_CONTROLLER = new PropertyDescriptor.Builder()
            .name("controller")
            .displayName("Pravega Controller URI")
            .description("The URI of the Pravega controller (e.g. tcp://localhost:9090).")
            .required(true)
            .addValidator(CONTROLLER_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_SCOPE = new PropertyDescriptor.Builder()
            .name("scope")
            .displayName("Pravega Scope")
            .description("The name of the Pravega scope.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_STREAM = new PropertyDescriptor.Builder()
            .name("stream")
            .displayName("Pravega Stream")
            .description("The name of the Pravega stream.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was sent to Pravega.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Pravega will be routed to this Relationship.")
            .build();

    ComponentLog logger;
    EventStreamWriter<byte[]> cachedWriter;
    ClientFactory cachedClientFactory;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_CONTROLLER);
        descriptors.add(PROP_SCOPE);
        descriptors.add(PROP_STREAM);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnStopped
    public void onStop(final ProcessContext context) {
        synchronized (this) {
            logger.debug("PublishPravega.onStop");
            if (cachedWriter != null) {
                cachedWriter.close();
                cachedWriter = null;
            }
            if (cachedClientFactory != null) {
                cachedClientFactory.close();
                cachedClientFactory = null;
            }
        }
    }

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

        logger.info("Sending {} messages in transaction {} to Pravega stream {}.",
                new Object[]{flowFiles.size(), txnId, transitUri});

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

                logger.debug("routingKey={}, size={}, messageContent={}",
                        new Object[]{routingKey, flowFile.getSize(), messageContent});

                // Write to Pravega.
                transaction.writeEvent(routingKey, messageContent);
            }
            transaction.commit();
        }
        catch (TxnFailedException e) {
            logger.error(e.getMessage());
            // Transfer the FlowFiles to the failure relationship.
            // The user can choose to route this back to this processor for retry.
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }

        final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        logger.info("Sent {} messages in {} milliseconds to Pravega stream {} in transaction {}.",
                new Object[]{flowFiles.size(), transmissionMillis, transitUri, txnId});

        // Transfer any successful FlowFiles.
        for (FlowFile flowFile : flowFiles) {
            session.getProvenanceReporter().send(flowFile, transitUri, transmissionMillis);
            session.transfer(flowFile, REL_SUCCESS);
        }

        logger.debug("PublishPravega.onTrigger: END");
    }

    /**
     * Builds transit URI for provenance event. The transit URI will be in the form of
     * tcp://localhost:9090/scope/stream.
     */
    static String buildTransitURI(String controller, String scope, String streamName) {
        return String.format("%s/%s/%s", controller, scope, streamName);
    }

    protected EventStreamWriter<byte[]> getWriter(ProcessContext context) {
        synchronized (this) {
            if (cachedWriter == null) {
                try {
                    final URI controllerURI = new URI(context.getProperty(PROP_CONTROLLER).getValue());
                    final String scope = context.getProperty(PROP_SCOPE).getValue();
                    final String streamName = context.getProperty(PROP_STREAM).getValue();

                    // TODO: Create scope and stream based on additional properties.
                    final StreamManager streamManager = StreamManager.create(controllerURI);
                    streamManager.createScope(scope);
                    final StreamConfiguration streamConfig = StreamConfiguration.builder()
                            .scalingPolicy(ScalingPolicy.fixed(1))
                            .build();
                    streamManager.createStream(scope, streamName, streamConfig);
                    streamManager.close();

                    cachedClientFactory = ClientFactory.withScope(scope, controllerURI);
                    cachedWriter = cachedClientFactory.createEventWriter(
                            streamName,
                            new ByteArraySerializer(),
                            EventWriterConfig.builder().build());
                    return cachedWriter;
                } catch (URISyntaxException e) {
                    logger.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            } else {
                return cachedWriter;
            }
        }
    }
}
