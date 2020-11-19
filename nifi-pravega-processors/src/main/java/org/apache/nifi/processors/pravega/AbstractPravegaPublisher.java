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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
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

    EventStreamClientFactory cachedClientFactory;
    TransactionalEventStreamWriter<byte[]> cachedWriter;

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

    protected TransactionalEventStreamWriter<byte[]> getWriter(ProcessContext context) {
        synchronized (this) {
            logger.debug("getWriter: this={}", new Object[]{System.identityHashCode(this)});
            if (cachedWriter == null) {
                ClientConfig clientConfig = getClientConfig(context);

                final Stream stream = getStream(context);
                logger.debug("getWriter: stream={}, this={}",
                        new Object[]{stream, System.identityHashCode(this)});
                final StreamConfiguration streamConfig = getStreamConfiguration(context);
                try (final StreamManager streamManager = StreamManager.create(clientConfig)) {
                    if(new Boolean(context.getProperty(PROP_LOCAL_PRAVEGA).getValue()))
                        streamManager.createScope(stream.getScope());

                    streamManager.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
                }
                final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(stream.getScope(), clientConfig);
                final TransactionalEventStreamWriter<byte[]> writer = clientFactory.createTransactionalEventWriter(
                        stream.getStreamName(),
                        new ByteArraySerializer(),
                        EventWriterConfig.builder().build());

                cachedClientFactory = clientFactory;
                cachedWriter = writer;
            }
            return cachedWriter;
        }
    }
}
