package org.apache.nifi.processors.pravega;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractPravegaPublisher extends AbstractPravegaProcessor {
    static final Set<Relationship> relationships;

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was sent to Pravega.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Pravega will be routed to this Relationship.")
            .build();

    ClientFactory cachedClientFactory;
    EventStreamWriter<byte[]> cachedWriter;

    static {
        final Set<Relationship> innerRelationshipsSet = new HashSet<>();
        innerRelationshipsSet.add(REL_SUCCESS);
        innerRelationshipsSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(innerRelationshipsSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnStopped
    public void onStop(final ProcessContext context) {
        synchronized (this) {
            logger.debug("onStop: this={}", new Object[]{this});
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

    protected EventStreamWriter<byte[]> getWriter(ProcessContext context) {
        synchronized (this) {
            logger.debug("getWriter: this={}", new Object[]{System.identityHashCode(this)});
            if (cachedWriter == null) {
                ClientConfig clientConfig = getClientConfig(context);
                final String scope = context.getProperty(PROP_SCOPE).getValue();
                final String streamName = context.getProperty(PROP_STREAM).getValue();
                logger.debug("getWriter: scope={}, streamName={}, this={}",
                        new Object[]{scope, streamName, System.identityHashCode(this)});
                final StreamConfiguration streamConfig = getStreamConfiguration(context);
                try (final StreamManager streamManager = StreamManager.create(clientConfig)) {
                    streamManager.createScope(scope);
                    streamManager.createStream(scope, streamName, streamConfig);
                }
                final ClientFactory clientFactory = ClientFactory.withScope(scope, clientConfig);
                final EventStreamWriter<byte[]> writer = clientFactory.createEventWriter(
                        streamName,
                        new ByteArraySerializer(),
                        EventWriterConfig.builder().build());

                cachedClientFactory = clientFactory;
                cachedWriter = writer;
            }
            return cachedWriter;
        }
    }
}
