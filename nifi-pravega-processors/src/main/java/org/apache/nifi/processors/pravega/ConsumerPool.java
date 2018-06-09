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
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
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
    private final ScheduledExecutorService performCheckpointExecutor;
    private final ScheduledExecutorService initiateCheckpointExecutor;
    private final StateManager stateManager;
    private final ProcessSessionFactory sessionFactory;
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
            final ProcessSessionFactory sessionFactory,
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
        this.sessionFactory = sessionFactory;
        this.isPrimaryNode = isPrimaryNode;
        this.maxWaitMillis = maxWaitMillis;
        this.controllerURI = controllerURI;
        this.scope = scope;
        this.streamNames = Collections.unmodifiableList(streamNames);
        this.streamConfig = streamConfig;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;

        final boolean primaryNode = isPrimaryNode.get();
        final StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        final String previousReaderGroup = stateMap.get(STATE_KEY_READER_GROUP);
        final boolean haveReaderGroup = previousReaderGroup != null;
        final String checkpointStr = stateMap.get(STATE_KEY_CHECKPOINT_BASE64);
        final boolean haveCheckpoint = checkpointStr != null;

        if (!haveReaderGroup && !primaryNode) {
            throw new RuntimeException("Non-primary node can't start until the reader group has been created.");
        }

        pooledLeases = new ArrayBlockingQueue<>(maxConcurrentLeases);
        clientFactory = ClientFactory.withScope(scope, controllerURI);
        try {
            performCheckpointExecutor = Executors.newScheduledThreadPool(1);
            try {
                initiateCheckpointExecutor = Executors.newScheduledThreadPool(1);
                try {
                    // Create reader group manager.
                    readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);
                    try {
                        if (haveReaderGroup) {
                            // Use the reader group from the previous state.
                            logger.debug("ConsumerPool: previous state={}", new Object[]{stateMap.toMap()});
                            final String prevReaderGroupName = stateMap.get(STATE_KEY_READER_GROUP);
                            if (prevReaderGroupName == null || prevReaderGroupName.isEmpty()) {
                                throw new Exception("State is missing the reader group name.");
                            }
                            readerGroupName = prevReaderGroupName;
                            logger.debug("ConsumerPool: Using existing reader group {}", new Object[]{readerGroupName});
                            readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
                        } else {
                            if (!primaryNode) {
                                throw new RuntimeException("bug");
                            }
                            // This processor is starting for the first time on the primary node.

                            // Create streams.
                            try (final StreamManager streamManager = StreamManager.create(controllerURI)) {
                                streamManager.createScope(scope);
                                for (String streamName : streamNames) {
                                    streamManager.createStream(scope, streamName, streamConfig);
                                }
                            }

                            // Create reader group.
                            readerGroupName = UUID.randomUUID().toString().replace("-", "");
                            logger.debug("ConsumerPool: Using new reader group {}", new Object[]{readerGroupName});
                            ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder()
                                    .disableAutomaticCheckpoints();
                            if (haveCheckpoint) {
                                logger.debug("ConsumerPool: Starting the reader group from checkpoint {}", new Object[]{checkpointStr});
                                final Checkpoint checkpoint = Checkpoint.fromBytes(ByteBuffer.wrap(Base64.getDecoder().decode(checkpointStr)));
                                builder = builder.startFromCheckpoint(checkpoint);
                            } else {
                                // Determine starting stream cuts.
                                final Map<Stream, StreamCut> startingStreamCuts = new HashMap<>();
                                for (String streamName : streamNames) {
                                    Stream stream = Stream.of(scope, streamName);
                                    // TODO: get tail stream cut if requested
                                    startingStreamCuts.put(stream, StreamCut.UNBOUNDED);
                                }
                                builder = builder.startFromStreamCuts(startingStreamCuts);
                            }
                            final ReaderGroupConfig readerGroupConfig = builder.build();
                            readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
                            readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
                        }
                    } catch (Exception e) {
                        readerGroupManager.close();
                        throw e;
                    }
                } catch (Exception e) {
                    initiateCheckpointExecutor.shutdown();
                    throw e;
                }
            } catch (Exception e) {
                performCheckpointExecutor.shutdown();
                throw e;
            }
        } catch (Exception e) {
            clientFactory.close();
            throw e;
        }

        logger.info("Using reader group {} to read from Pravega scope {}, streams {}.",
                new Object[]{readerGroupName, scope, streamNames});

        // Schedule periodic task to initiate checkpoints.
        performCheckpointExecutor.scheduleWithFixedDelay(this::performRegularCheckpoint, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    private final Object checkpointMutex = new Object();

    private void performRegularCheckpoint() {
        performCheckpoint(false, null);
    }

    /**
     * Perform a checkpoint, wait for it to complete, and write the checkpoint to the state.
     *
     * @param isFinal If true, the processor is shutting down and this is the primary node.
     *                This will be the final checkpoint.
     * @param context Required if isFinal is true.
     */
    private void performCheckpoint(final boolean isFinal, final ProcessContext context) {
        logger.debug("performCheckpoint: BEGIN");
        synchronized (checkpointMutex) {
            try {
                if (isFinal || isPrimaryNode.get()) {
                    final String checkpointName = (isFinal ? CHECKPOINT_NAME_FINAL_PREFIX : "") + UUID.randomUUID().toString();
                    logger.debug("performCheckpoint: Calling initiateCheckpoint; checkpointName={}", new Object[]{checkpointName});
                    CompletableFuture<Checkpoint> checkpointFuture = readerGroup.initiateCheckpoint(checkpointName, initiateCheckpointExecutor);
                    logger.debug("performCheckpoint: Got future.");
                    Checkpoint checkpoint = checkpointFuture.get();
                    logger.debug("performCheckpoint: Checkpoint completed; checkpoint={}, positions={}", new Object[]{checkpoint, checkpoint.asImpl().getPositions()});
                    if (isFinal || isPrimaryNode.get()) {
                        // Get serialized checkpoint byte array and convert to base64 string.
                        String checkpointStr = new String(Base64.getEncoder().encode(checkpoint.toBytes().array()));
                        Map<String, String> mapState = new HashMap<>();
                        // The final checkpoint does not have a reader group.
                        if (!isFinal) {
                            mapState.put(STATE_KEY_READER_GROUP, readerGroupName);
                        }
                        mapState.put(STATE_KEY_CHECKPOINT_BASE64, checkpointStr);
                        mapState.put(STATE_KEY_CHECKPOINT_NAME, checkpoint.getName());
                        mapState.put(STATE_KEY_CHECKPOINT_TIME, ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
                        stateManager.setState(mapState, Scope.CLUSTER);
                    }
                }
            } catch (final Exception e) {
                logger.warn("performCheckpoint: {}", new Object[]{e.getMessage()});
                // Ignore error. We will retry when we are scheduled again.
            }
        }
        logger.debug("performCheckpoint: END");
    }

    final static String CHECKPOINT_NAME_FINAL_PREFIX = "FINAL-";

    /**
     * We want to gracefully stop this processor.
     * This means that the last checkpoint written to the state must
     * represent exactly what each onTrigger thread on each node committed to each session.
     * If any checkpoint is in progress, then it is possible for some threads to already have committed their session.
     * Therefore, if any checkpoint is in progress, we want to wait for it and ensure that another one does not start.
     *
     */
    public void gracefulShutdown(final ProcessContext context) {
        logger.info("gracefulShutdown: BEGIN");
        if (isPrimaryNode.get()) {
            // onTrigger may not be executed anymore so we need to process events until the final checkpoint is reached.
            ExecutorService drainExecutor = startDrainLeases(context);
            try {
                // Shutdown checkpoint scheduler so that it doesn't start a new checkpoint.
                performCheckpointExecutor.shutdown();
                try {
                    logger.debug("gracefulShutdown: waiting for checkpoint scheduler executor to terminate");
                    performCheckpointExecutor.awaitTermination(60000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error("gracefulShutdown", e);
                }
                performCheckpoint(true, context);
            } finally {
                drainExecutor.shutdown();
            }
            // Delete reader group.
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }
        logger.info("gracefulShutdown: END");
    }

    // In case no onTrigger threads are running, we go through each lease and run it until the final checkpoint is received.
    private ExecutorService startDrainLeases(final ProcessContext context) {
        ScheduledExecutorService drainExecutor = Executors.newScheduledThreadPool(1);
        // TODO: do more than one?
        drainExecutor.scheduleWithFixedDelay(() -> drainLeases(context), 0, 1000, TimeUnit.MILLISECONDS);
        return drainExecutor;
    }

    private void drainLeases(final ProcessContext context) {
        logger.debug("drainLeases: BEGIN");
        for (;;) {
            try (final SimpleConsumerLease lease = pooledLeases.poll()) {
                if (lease == null) {
                    break;
                }
                logger.debug("drainLeases: draining lease {}", new Object[]{lease.toString()});
                final ProcessSession session = sessionFactory.createSession();
                lease.setProcessSession(session, context);
                lease.readEvents();
            }
        }
        logger.debug("drainLeases: END");
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
        // Attempt to get lease (reader) from queue.
        SimpleConsumerLease lease = pooledLeases.poll();
        if (lease == null) {
            final String readerId = generateReaderId();
            final EventStreamReader<byte[]> reader = createPravegaReader(readerId);
            consumerCreatedCountRef.incrementAndGet();
            /**
             * For now return a new consumer lease. But we could later elect to
             * have this return null if we determine the broker indicates that
             * the lag time on all streams being monitored is sufficiently low.
             * For now we should encourage conservative use of threads because
             * having too many means we'll have at best useless threads sitting
             * around doing frequent network calls and at worst having consumers
             * sitting idle which could prompt excessive rebalances.
             */
            lease = new SimpleConsumerLease(reader, readerId);
        }
        // Bind the reader in the lease to this session.
        lease.setProcessSession(session, processContext);

        leasesObtainedCountRef.incrementAndGet();
        return lease;
    }

    protected String generateReaderId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * Exposed as protected method for easier unit testing
     *
     * @return reader
     */
    protected EventStreamReader<byte[]> createPravegaReader(final String readerId) {
        logger.debug("createPravegaReader: readerId={}", new Object[]{readerId});
        final EventStreamReader<byte[]> reader = clientFactory.createReader(
                readerId,
                readerGroupName,
                new ByteArraySerializer(),
                ReaderConfig.builder().build());
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
        performCheckpointExecutor.shutdown();
        initiateCheckpointExecutor.shutdown();
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

        private SimpleConsumerLease(final EventStreamReader<byte[]> reader, final String readerId) {
            super(reader, readerId, readerFactory, writerFactory, logger);
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
