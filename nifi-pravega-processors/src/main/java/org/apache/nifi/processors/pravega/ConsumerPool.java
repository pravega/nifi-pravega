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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
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

import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.nifi.processors.pravega.ConsumePravega.STREAM_CUT_EARLIEST;
import static org.apache.nifi.processors.pravega.ConsumePravega.STREAM_CUT_LATEST;

/**
 * A pool of Pravega Readers for a given stream.
 * This maps to a Pravega Reader Group.
 * Multiple instances of a component in a NiFi cluster will share a single reader group.
 */
public class ConsumerPool implements AutoCloseable {

    static final String CHECKPOINT_NAME_FINAL_PREFIX = "FINAL-";
    static private final String STATE_KEY_READER_GROUP = "reader.group.name";
    static private final String STATE_KEY_CHECKPOINT_NAME = "reader.group.checkpoint.name";
    static private final String STATE_KEY_CHECKPOINT_BASE64 = "reader.group.checkpoint.base64";
    static private final String STATE_KEY_CHECKPOINT_TIME = "reader.group.checkpoint.time";
    final StreamConfiguration streamConfig;
    private final BlockingQueue<SimpleConsumerLease> pooledLeases;      // must have lock on activeLeases to access
    private final Set<SimpleConsumerLease> activeLeases = new HashSet<>();
    private final ClientConfig clientConfig;
    private final String scope;
    private final List<Stream> streams;
    private final int maxConcurrentLeases;
    private final long checkpointPeriodMs;
    private final long checkpointTimeoutMs;
    private final long gracefulShutdownTimeoutMs;
    private final long minimumProcessingTimeMs;
    private final ComponentLog logger;
    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final AtomicLong consumerCreatedCountRef = new AtomicLong();
    private final AtomicLong consumerClosedCountRef = new AtomicLong();
    private final AtomicLong leasesObtainedCountRef = new AtomicLong();
    private final String readerGroupName;
    private final ReaderGroupManager readerGroupManager;
    private final ReaderGroup readerGroup;
    private final ScheduledExecutorService performCheckpointExecutor;
    private final ScheduledExecutorService initiateCheckpointExecutor;
    private final StateManager stateManager;
    private final ProcessSessionFactory sessionFactory;
    private final Supplier<Boolean> isPrimaryNode;
    private final Object checkpointMutex = new Object();
    private final boolean localPravega;

    /**
     * Creates a pool of Pravega reader objects that will grow up to the maximum
     * indicated threads from the given context. Consumers are lazily
     * initialized. We may elect to not create up to the maximum number of
     * configured consumers if the broker reported lag time for all streamNames is
     * below a certain threshold.
     *
     * @param maxConcurrentLeases max allowable consumers at once
     * @param logger              the logger to report any errors/warnings
     */
    public ConsumerPool(
            final ComponentLog logger,
            final StateManager stateManager,
            final ProcessSessionFactory sessionFactory,
            final Supplier<Boolean> isPrimaryNode,
            final int maxConcurrentLeases,
            final long checkpointPeriodMs,
            final long checkpointTimeoutMs,
            final long gracefulShutdownTimeoutMs,
            final long minimumProcessingTimeMs,
            final ClientConfig clientConfig,
            final List<Stream> streams,
            final StreamConfiguration streamConfig,
            final String streamCutMethod,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final boolean localPravega) throws Exception {
        this.logger = logger;
        this.stateManager = stateManager;
        this.sessionFactory = sessionFactory;
        this.isPrimaryNode = isPrimaryNode;
        this.maxConcurrentLeases = maxConcurrentLeases;
        this.checkpointPeriodMs = checkpointPeriodMs;
        this.checkpointTimeoutMs = checkpointTimeoutMs;
        this.gracefulShutdownTimeoutMs = gracefulShutdownTimeoutMs;
        this.minimumProcessingTimeMs = minimumProcessingTimeMs;
        this.clientConfig = clientConfig;
        this.scope = streams.get(0).getScope();
        this.streams = Collections.unmodifiableList(streams);
        this.streamConfig = streamConfig;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.localPravega = localPravega;

        final boolean primaryNode = isPrimaryNode.get();
        final StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        final String previousReaderGroup = stateMap.get(STATE_KEY_READER_GROUP);
        final boolean haveReaderGroup = previousReaderGroup != null;
        final String checkpointStr = stateMap.get(STATE_KEY_CHECKPOINT_BASE64);
        final boolean haveCheckpoint = checkpointStr != null;

        if (!haveReaderGroup && !primaryNode) {
            throw new ProcessorNotReadyException("Non-primary node can't start until the reader group has been created.");
        }

        pooledLeases = new ArrayBlockingQueue<>(maxConcurrentLeases);
        performCheckpointExecutor = Executors.newScheduledThreadPool(1);
        try {
            initiateCheckpointExecutor = Executors.newScheduledThreadPool(1);
            try {
                // Create reader group manager.
                readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
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
                        try (final StreamManager streamManager = StreamManager.create(clientConfig)) {
                            for (Stream stream : streams) {
                                if (localPravega)
                                    streamManager.createScope(stream.getScope());

                                streamManager.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
                            }

                            // Create reader group.
                            readerGroupName = UUID.randomUUID().toString().replace("-", "");
                            logger.debug("ConsumerPool: Using new reader group {}", new Object[]{readerGroupName});
                            ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder()
                                    .disableAutomaticCheckpoints();
                            logger.debug("ConsumerPool: ReaderGroupConfigBuilder {}", new Object[]{builder});
                            if (haveCheckpoint) {
                                logger.debug("ConsumerPool: Starting the reader group from checkpoint {}", new Object[]{checkpointStr});
                                final Checkpoint checkpoint = Checkpoint.fromBytes(ByteBuffer.wrap(Base64.getDecoder().decode(checkpointStr)));
                                builder = builder.startFromCheckpoint(checkpoint);
                                logger.debug("ConsumerPool: Inside If(haveCheckpoint) ReaderGroupConfigBuilder {}", new Object[]{builder});
                            } else {
                                // Determine starting stream cuts.
                                final Map<Stream, StreamCut> startingStreamCuts = new HashMap<>();
                                if (streamCutMethod.equals(STREAM_CUT_LATEST.getValue())) {
                                    for (Stream stream : streams) {
                                        StreamInfo streamInfo = streamManager.getStreamInfo(stream.getScope(), stream.getStreamName());
                                        StreamCut tailStreamCut = streamInfo.getTailStreamCut();
                                        startingStreamCuts.put(stream, tailStreamCut);
                                    }
                                } else if (streamCutMethod.equals(STREAM_CUT_EARLIEST.getValue())) {
                                    for (Stream stream : streams) {
                                        startingStreamCuts.put(stream, StreamCut.UNBOUNDED);
                                    }
                                } else {
                                    throw new Exception("Invalid stream cut method " + streamCutMethod);
                                }
                                logger.debug("ConsumerPool: startingStreamCuts={}", new Object[]{startingStreamCuts});
                                builder = builder.startFromStreamCuts(startingStreamCuts);
                            }
                            final ReaderGroupConfig readerGroupConfig = builder.build();
                            readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
                            readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    readerGroupManager.close();
                    throw e;
                }
            } catch (Exception e) {
                e.printStackTrace();
                initiateCheckpointExecutor.shutdown();
                throw e;
            }
        } catch (Exception e) {
            e.printStackTrace();
            performCheckpointExecutor.shutdown();
            throw e;
        }

        logger.info("Using reader group {} to read from {}.",
                new Object[]{readerGroupName, streams});

        // Schedule periodic task to initiate checkpoints.
        // If any execution of this task takes longer than its period, then subsequent executions may start late, but will not concurrently execute.
        performCheckpointExecutor.scheduleAtFixedRate(this::performRegularCheckpoint, checkpointPeriodMs, checkpointPeriodMs, TimeUnit.MILLISECONDS);

        logger.debug("Created {}", new Object[]{this.getConfigAsString()});
    }

    public String getConfigAsString() {
        return "ConsumerPool{" +
                "clientConfig=" + clientConfig +
                ", maxConcurrentLeases=" + maxConcurrentLeases +
                ", scope='" + scope + '\'' +
                ", streams=" + streams +
                ", streamConfig=" + streamConfig +
                ", checkpointPeriodMs=" + checkpointPeriodMs +
                ", checkpointTimeoutMs=" + checkpointTimeoutMs +
                ", gracefulShutdownTimeoutMs=" + gracefulShutdownTimeoutMs +
                ", minimumProcessingTimeMs=" + minimumProcessingTimeMs +
                '}';
    }

    private void performRegularCheckpoint() {
        performCheckpoint(false, null);
    }

    /**
     * Perform a checkpoint, wait for it to complete, and write the checkpoint to the state.
     *
     * @param isFinal If true, a final checkpoint will be performed.
     * @param context Required if isFinal is true.
     */
    private void performCheckpoint(final boolean isFinal, final ProcessContext context) {
        logger.debug("performCheckpoint: BEGIN");
        synchronized (checkpointMutex) {
            try {
                if (isPrimaryNode.get()) {
                    logger.debug("performCheckpoint: this is the primary node");
                    final Set<String> onlineReaders = readerGroup.getOnlineReaders();
                    logger.debug("performCheckpoint: onlineReaders ({})={}", new Object[]{onlineReaders.size(), onlineReaders});
                    final String checkpointName = (isFinal ? CHECKPOINT_NAME_FINAL_PREFIX : "") + UUID.randomUUID().toString();
                    logger.debug("performCheckpoint: Calling initiateCheckpoint; checkpointName={}", new Object[]{checkpointName});
                    CompletableFuture<Checkpoint> checkpointFuture = readerGroup.initiateCheckpoint(checkpointName, initiateCheckpointExecutor);
                    logger.debug("performCheckpoint: Got future.");
                    Checkpoint checkpoint = checkpointFuture.get(checkpointTimeoutMs, TimeUnit.MILLISECONDS);
                    logger.debug("performCheckpoint: Checkpoint completed; checkpoint={}, positions={}", new Object[]{checkpoint, checkpoint.asImpl().getPositions()});
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
            } catch (final Exception e) {
                logger.warn("performCheckpoint: timed out waiting for checkpoint to complete", e);
                // Ignore error. We will retry when we are scheduled again.
            }
        }
        logger.debug("performCheckpoint: END");
    }

    /**
     * We want to gracefully stop this processor.
     * This means that the last checkpoint written to the state must
     * represent exactly what each thread on each node committed to each session.
     * If any checkpoint is in progress, then it is possible for some threads to already have committed their session.
     * Therefore, if any checkpoint is in progress, we want to wait for it and ensure that another one does not start.
     * <p>
     * When the primary cluster node is shut down, it is no longer the primary node when this method is called.
     * <p>
     * Note that this is called by onUnscheduled which is called when the user stops the processor.
     * However, it is also called when a NiFi node is stopped in a cluster, in which case we actually
     * want the other nodes to continue running. So we want this method to perform everything needed
     * for a graceful shutdown but to allow other nodes to continue using the reader group.
     * Therefore, the reader group is not deleted by this processor.
     * Instead it should be cleaned up periodically by Pravega.
     */
    public void gracefulShutdown(final ProcessContext context) {
        logger.debug("gracefulShutdown: BEGIN");
        // Shutdown checkpoint scheduler so that it doesn't start a new checkpoint.
        performCheckpointExecutor.shutdown();
        // Create an executor that will run the shutdown tasks concurrently.
        ExecutorService gracefulShutdownExecutor = Executors.newCachedThreadPool();
        try {
            // Start a concurrent task to perform the final checkpoint.
            gracefulShutdownExecutor.submit(() -> {
                logger.debug("gracefulShutdown: waiting for checkpoint scheduler executor to terminate");
                try {
                    // If a checkpoint is already in progress, wait for it.
                    performCheckpointExecutor.awaitTermination(checkpointTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error("gracefulShutdown", e);
                }
                // Perform the final checkpoint.
                performCheckpoint(true, context);
                logger.info("Graceful shutdown completed successfully.");
            });

            // Drain all leases until each one has reached a final checkpoint.
            for (; ; ) {
                synchronized (activeLeases) {
                    // If we have any leases in pooledLeases, they will not be drained by onTrigger so we
                    // submit a task to drain each one.
                    final List<SimpleConsumerLease> leases = new ArrayList<>();
                    pooledLeases.drainTo(leases);
                    leases.forEach((lease) -> {
                        gracefulShutdownExecutor.submit(() -> {
                            logger.debug("gracefulShutdown: draining lease " + lease);
                            final ProcessSession session = sessionFactory.createSession();
                            lease.setProcessSession(session, context);
                            try {
                                lease.readEventsUntilFinalCheckpoint();
                            } catch (Exception e) {
                                logger.error("gracefulShutdown: Exception", e);
                            }
                            lease.close(true);
                        });
                    });
                    // If we still have an active lease, an onTrigger thread is still running.
                    // We need to wait for it to complete and put the lease back into the pool.
                    if (activeLeases.isEmpty()) {
                        break;
                    }
                    logger.debug("gracefulShutdown: activeLeases count=" + activeLeases.size());
                }
                Thread.sleep(100);
            }

            // Wait for above tasks to complete (possibly with an exception).
            gracefulShutdownExecutor.shutdown();
            gracefulShutdownExecutor.awaitTermination(gracefulShutdownTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            gracefulShutdownExecutor.shutdownNow();
        }
        logger.debug("gracefulShutdown: END");
    }

    /**
     * Obtains a consumer from the pool if one is available or lazily
     * initializes a new one if deemed necessary.
     *
     * @param session        the session for which the consumer lease will be
     *                       associated
     * @param processContext the ProcessContext for which the consumer
     *                       lease will be associated
     * @return consumer to use or null if not available or necessary
     */
    public ConsumerLease obtainConsumer(final ProcessSession session, final ProcessContext processContext) {
        // Attempt to get lease (reader) from queue.
        synchronized (activeLeases) {
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

            activeLeases.add(lease);
            leasesObtainedCountRef.incrementAndGet();
            return lease;
        }
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
        final EventStreamReader<byte[]> reader = EventStreamClientFactory.withScope(scope, clientConfig).createReader(
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
        // Leases should have been closed in gracefulShutdown. In case they were not, try again.
        final List<SimpleConsumerLease> leases = new ArrayList<>();
        synchronized (activeLeases) {
            pooledLeases.drainTo(leases);
        }
        leases.stream().forEach((lease) -> {
            lease.close(true);
        });
        performCheckpointExecutor.shutdownNow();
        initiateCheckpointExecutor.shutdownNow();
        try {
            performCheckpointExecutor.awaitTermination(checkpointTimeoutMs, TimeUnit.MILLISECONDS);
            initiateCheckpointExecutor.awaitTermination(checkpointTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("ConsumerPool.close: Exception", e);
        }
        readerGroup.close();
        readerGroupManager.close();
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

    private class SimpleConsumerLease extends ConsumerLease {

        private volatile ProcessSession session;
        private volatile ProcessContext processContext;
        private volatile boolean closedConsumer;

        private SimpleConsumerLease(final EventStreamReader<byte[]> reader, final String readerId) {
            super(
                    clientConfig,
                    reader,
                    readerId,
                    checkpointTimeoutMs,
                    minimumProcessingTimeMs,
                    readerFactory,
                    writerFactory,
                    logger);
        }

        void setProcessSession(final ProcessSession session, final ProcessContext context) {
            this.session = session;
            this.processContext = context;
        }

        @Override
        public ProcessSession getProcessSession() {
            return session;
        }

        @Override
        public void yield() {
            if (processContext != null) {
                processContext.yield();
            }
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
            boolean addedToPool = false;
            synchronized (activeLeases) {
                activeLeases.remove(this);
                if (!(forceClose || isPoisoned())) {
                    addedToPool = pooledLeases.offer(this);
                }
            }
            if (!addedToPool) {
                closedConsumer = true;
                closeReader(reader);
                logger.debug("SimpleConsumerLease.close: Reader {} closed", new Object[]{readerId});
            }
        }
    }

}
