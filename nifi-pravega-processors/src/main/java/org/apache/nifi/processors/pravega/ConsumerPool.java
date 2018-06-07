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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A pool of Pravega Readers for a given stream. Consumers can be obtained by
 * calling 'obtainConsumer'.
 * This maps to a Pravega Reader Group.
 */
public class ConsumerPool implements AutoCloseable {

    private final BlockingQueue<SimpleConsumerLease> pooledLeases;
    private final URI controllerURI;
    private final String scope;
    private final List<String> streamNames;
    final StreamConfiguration streamConfig;
    // TODO: add variables to identify where in streams to start (head, tail, stream cuts)
    private final long maxWaitMillis;
    private final ComponentLog logger;
    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final AtomicLong consumerCreatedCountRef = new AtomicLong();
    private final AtomicLong consumerClosedCountRef = new AtomicLong();
    private final AtomicLong leasesObtainedCountRef = new AtomicLong();
    private final String readerGroupName;
    private final ClientFactory clientFactory;
    private final ReaderGroupManager readerGroupManager;
    private final ReaderGroup readerGroup;
    private final ScheduledExecutorService scheduler;
    private final StateManager stateManager;
    private final Supplier<Boolean> isPrimaryNode;

    static private final String STATE_KEY_READER_GROUP = "reader.group.name";
    static private final String STATE_KEY_CHECKPOINT_NAME = "reader.group.checkpoint.name";
    static private final String STATE_KEY_CHECKPOINT_BASE64 = "reader.group.checkpoint.base64";
    static private final String STATE_KEY_CHECKPOINT_TIME = "reader.group.checkpoint.time";

    /**
     * Creates a pool of Pravega reader objects that will grow up to the maximum
     * indicated threads from the given context. Consumers are lazily
     * initialized. We may elect to not create up to the maximum number of
     * configured consumers if the broker reported lag time for all streamNames is
     * below a certain threshold.
     *
     * @param maxConcurrentLeases max allowable consumers at once
     * @param streamNames the streamNames to subscribe to
     * @param maxWaitMillis maximum time to wait for a given lease to acquire
     * data before committing
     * @param logger the logger to report any errors/warnings
     */
    public ConsumerPool(
            final ComponentLog logger,
            final StateManager stateManager,
            final Supplier<Boolean> isPrimaryNode,
            final int maxConcurrentLeases,
            final long maxWaitMillis,
            final URI controllerURI,
            final String scope,
            final List<String> streamNames,
            final StreamConfiguration streamConfig,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory) throws Exception {
        this.logger = logger;
        this.stateManager = stateManager;
        this.isPrimaryNode = isPrimaryNode;
        this.maxWaitMillis = maxWaitMillis;
        this.controllerURI = controllerURI;
        this.scope = scope;
        this.streamNames = Collections.unmodifiableList(streamNames);
        this.streamConfig = streamConfig;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;

        boolean primaryNode = isPrimaryNode.get();

        StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        boolean havePreviousState = !stateMap.toMap().isEmpty();

        if (!havePreviousState && !primaryNode) {
            throw new RuntimeException("Non-primary node can't start until state exists. Try again later.");
        }

        pooledLeases = new ArrayBlockingQueue<>(maxConcurrentLeases);

        clientFactory = ClientFactory.withScope(scope, controllerURI);
        try {
            scheduler = Executors.newScheduledThreadPool(1);
            try {
                // Create reader group manager.
                readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);
                try {
                    if (!havePreviousState && primaryNode) {
                        // This processor is starting for the first time on the primary node.
                        readerGroupName = UUID.randomUUID().toString().replace("-", "");
                        logger.debug("ConsumerPool: Using new reader group {}", new Object[]{readerGroupName});

                        // Create streams.
                        try (final StreamManager streamManager = StreamManager.create(controllerURI)) {
                            streamManager.createScope(scope);
                            for (String streamName : streamNames) {
                                streamManager.createStream(scope, streamName, streamConfig);
                            }
                        }

                        // Determine starting stream cuts.
                        final Map<Stream, StreamCut> startingStreamCuts = new HashMap<>();
                        for (String streamName : streamNames) {
                            Stream stream = Stream.of(scope, streamName);
                            // TODO: get tail stream cut if requested
                            startingStreamCuts.put(stream, StreamCut.UNBOUNDED);
                        }

                        // Create reader group.
                        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                .startFromStreamCuts(startingStreamCuts)
                                .disableAutomaticCheckpoints()
                                .build();
                        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
                        readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
                    } else if (havePreviousState && primaryNode) {
                        // This could occur if user stopped and started this processor.
                        // Use previous group and reset it to our checkpoint.
                        logger.debug("ConsumerPool: previous state={}", new Object[]{stateMap.toMap()});
                        final String prevReaderGroupName = stateMap.get(STATE_KEY_READER_GROUP);
                        if (prevReaderGroupName == null || prevReaderGroupName.isEmpty()) {
                            throw new Exception("State is missing the reader group name.");
                        }
                        readerGroupName = prevReaderGroupName;
                        logger.debug("ConsumerPool: Using existing reader group {}", new Object[]{readerGroupName});
                        readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
                        try {
                            final String checkpointStr = stateMap.get(STATE_KEY_CHECKPOINT_BASE64);
                            logger.debug("ConsumerPool: Resetting the reader group to checkpoint {}", new Object[]{checkpointStr});
                            final Checkpoint checkpoint = Checkpoint.fromBytes(ByteBuffer.wrap(Base64.getDecoder().decode(checkpointStr)));
                            final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                    .startFromCheckpoint(checkpoint)
                                    .disableAutomaticCheckpoints()
                                    .build();
                            readerGroup.resetReaderGroup(readerGroupConfig);
                        } catch (Exception e) {
                            readerGroup.close();
                            throw e;
                        }
                    } else /*if (havePreviousState && !primaryNode)*/ {
                        // This processor is starting on a second node.
                        // Use the existing reader group as is.
//                        throw new Exception("unsupported");
                        logger.debug("ConsumerPool: previous state={}", new Object[]{stateMap.toMap()});
                        final String prevReaderGroupName = stateMap.get(STATE_KEY_READER_GROUP);
                        if (prevReaderGroupName == null || prevReaderGroupName.isEmpty()) {
                            throw new Exception("State is missing the reader group name.");
                        }
                        readerGroupName = prevReaderGroupName;
                        logger.debug("ConsumerPool: Using existing reader group {}", new Object[]{readerGroupName});
                        readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
                    }
                } catch (Exception e) {
                    readerGroupManager.close();
                    throw e;
                }
            } catch (Exception e) {
                scheduler.shutdown();
                throw e;
            }
        } catch (Exception e) {
            clientFactory.close();
            throw e;
        }

        logger.info("Using reader group {} to read from Pravega scope {}, streams {}.",
                new Object[]{readerGroupName, scope, streamNames});

        // Schedule periodic task to initiate checkpoints.
        scheduler.schedule(this::initiateCheckpoint, 1000, TimeUnit.MILLISECONDS);
    }

    void initiateCheckpoint() {
        if (isPrimaryNode.get()) {
            final String checkpointName = UUID.randomUUID().toString();
            logger.debug("initiateCheckpoint: Calling initiateCheckpoint(checkpointName={})", new Object[]{checkpointName});
            CompletableFuture<Checkpoint> checkpointFuture = readerGroup.initiateCheckpoint(checkpointName, scheduler);
            logger.debug("initiateCheckpoint: Got future.");
            checkpointFuture.whenComplete(this::afterCheckpoint);
        } else {
            scheduler.schedule(this::initiateCheckpoint, 1000, TimeUnit.MILLISECONDS);
        }
    }

    void afterCheckpoint(Checkpoint checkpoint, Throwable ex) {
        if (ex == null) {
            logger.info("afterCheckpoint: checkpoint={}, positions={}", new Object[]{checkpoint, checkpoint.asImpl().getPositions()});
            if (isPrimaryNode.get()) {
                // Get serialized checkpoint byte array and convert to base64 string.
                String checkpointStr = new String(Base64.getEncoder().encode(checkpoint.toBytes().array()));
                Map<String, String> mapState = new HashMap<>();
                mapState.put(STATE_KEY_READER_GROUP, readerGroupName);
                mapState.put(STATE_KEY_CHECKPOINT_BASE64, checkpointStr);
                mapState.put(STATE_KEY_CHECKPOINT_NAME, checkpoint.getName());
                mapState.put(STATE_KEY_CHECKPOINT_TIME, ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
                try {
                    stateManager.setState(mapState, Scope.CLUSTER);
                } catch (IOException e) {
                    logger.warn("afterCheckpoint: setState exception={}", new Object[]{e.getMessage()});
                }
            }
        } else {
            logger.warn("afterCheckpoint: Checkpoint exception={}", new Object[]{ex.getMessage()});
        }
        scheduler.schedule(this::initiateCheckpoint, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * Obtains a consumer from the pool if one is available or lazily
     * initializes a new one if deemed necessary.
     *
     * @param session the session for which the consumer lease will be
     *            associated
     * @param processContext the ProcessContext for which the consumer
     *            lease will be associated
     * @return consumer to use or null if not available or necessary
     */
    public ConsumerLease obtainConsumer(final ProcessSession session, final ProcessContext processContext) {
        SimpleConsumerLease lease = pooledLeases.poll();
        if (lease == null) {
            final EventStreamReader<byte[]> reader = createPravegaReader();
            consumerCreatedCountRef.incrementAndGet();
            /**
             * For now return a new consumer lease. But we could later elect to
             * have this return null if we determine the broker indicates that
             * the lag time on all streamNames being monitored is sufficiently low.
             * For now we should encourage conservative use of threads because
             * having too many means we'll have at best useless threads sitting
             * around doing frequent network calls and at worst having consumers
             * sitting idle which could prompt excessive rebalances.
             */
            lease = new SimpleConsumerLease(reader);
        }
        lease.setProcessSession(session, processContext);

        leasesObtainedCountRef.incrementAndGet();
        return lease;
    }

    /**
     * Exposed as protected method for easier unit testing
     *
     * @return reader
     */
    protected EventStreamReader<byte[]> createPravegaReader() {
        final String readerId = UUID.randomUUID().toString().replace("-", "");
        final EventStreamReader<byte[]> reader = clientFactory.createReader(
                readerId,
                readerGroupName,
                new ByteArraySerializer(),
                ReaderConfig.builder().build());
        logger.debug("createPravegaReader: readerId={}", new Object[]{readerId});
        return reader;
    }

    /**
     * Closes all consumers in the pool.
     */
    @Override
    public void close() {
        final List<SimpleConsumerLease> leases = new ArrayList<>();
        pooledLeases.drainTo(leases);
        leases.stream().forEach((lease) -> {
            lease.close(true);
        });
        scheduler.shutdown();
        readerGroup.close();
        readerGroupManager.close();
        clientFactory.close();
    }

    private void closeReader(final EventStreamReader<byte[]> reader) {
        consumerClosedCountRef.incrementAndGet();
        try {
            reader.close();
        } catch (Exception e) {
            logger.warn("Failed while closing " + reader, e);
        }
    }

    PoolStats getPoolStats() {
        return new PoolStats(consumerCreatedCountRef.get(), consumerClosedCountRef.get(), leasesObtainedCountRef.get());
    }

    private class SimpleConsumerLease extends ConsumerLease {

        private final EventStreamReader<byte[]> reader;
        private volatile ProcessSession session;
        private volatile ProcessContext processContext;
        private volatile boolean closedConsumer;

        private SimpleConsumerLease(final EventStreamReader<byte[]> reader) {
            super(maxWaitMillis, reader, readerFactory, writerFactory, logger);
            this.reader = reader;
        }

        void setProcessSession(final ProcessSession session, final ProcessContext context) {
            this.session = session;
            this.processContext = context;
        }

        @Override
        public void yield() {
            if (processContext != null) {
                processContext.yield();
            }
        }

        @Override
        public ProcessSession getProcessSession() {
            return session;
        }

        @Override
        public void close() {
            super.close();
            close(false);
        }

        public void close(final boolean forceClose) {
            if (closedConsumer) {
                return;
            }
            super.close();
            if (session != null) {
                session.rollback();
                setProcessSession(null, null);
            }
            if (forceClose || isPoisoned() || !pooledLeases.offer(this)) {
                closedConsumer = true;
                closeReader(reader);
            }
        }
    }

    static final class PoolStats {

        final long consumerCreatedCount;
        final long consumerClosedCount;
        final long leasesObtainedCount;

        PoolStats(
                final long consumerCreatedCount,
                final long consumerClosedCount,
                final long leasesObtainedCount
        ) {
            this.consumerCreatedCount = consumerCreatedCount;
            this.consumerClosedCount = consumerClosedCount;
            this.leasesObtainedCount = leasesObtainedCount;
        }

        @Override
        public String toString() {
            return "Created Consumers [" + consumerCreatedCount + "]\n"
                    + "Closed Consumers  [" + consumerClosedCount + "]\n"
                    + "Leases Obtained   [" + leasesObtainedCount + "]\n";
        }

    }

}
