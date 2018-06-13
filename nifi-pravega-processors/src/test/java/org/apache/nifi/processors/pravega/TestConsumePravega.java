package org.apache.nifi.processors.pravega;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class TestConsumePravega {
    private static Logger log = LoggerFactory.getLogger(TestConsumePravega.class);

    @Test
    @Ignore()
    public void testProcessor() throws Exception {
        // TODO: write tests

        final URI controllerURI = new URI("tcp://localhost:9090");
        final String scope = "TestConsumePravega-" + UUID.randomUUID().toString();
        final String streamName = "stream1";
        final StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        // Create scope and stream.
        try (final StreamManager streamManager = StreamManager.create(controllerURI)) {
            streamManager.createScope(scope);
            streamManager.createStream(scope, streamName, streamConfig);
        }

        // Write some data to the stream.
        try (final ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI)) {
            try (final EventStreamWriter<byte[]> writer = clientFactory.createEventWriter(
                    streamName,
                    new ByteArraySerializer(),
                    EventWriterConfig.builder().build())) {
                writer.writeEvent("routingkey1", new byte[]{0, 1, 3});
            }
        }

        // Create reader group.
        final String readerGroupName = UUID.randomUUID().toString().replace("-", "");;
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED)
                .disableAutomaticCheckpoints()
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);
        }

        // Start reader and leader threads.
        Thread readerThread = new Thread(() -> ReaderThread(controllerURI, scope, readerGroupName));
        readerThread.start();

        // Wait for reader to read first event.
        Thread.sleep(1000);

        Thread leaderThread = new Thread(() -> LeaderThread(controllerURI, scope, readerGroupName));
        leaderThread.start();

        leaderThread.join();

        // Write some data to the stream.
        try (final ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI)) {
            try (final EventStreamWriter<byte[]> writer = clientFactory.createEventWriter(
                    streamName,
                    new ByteArraySerializer(),
                    EventWriterConfig.builder().build())) {
                writer.writeEvent("routingkey1", new byte[]{42});
            }
        }
        readerThread.join();

        log.info("DONE");
    }

    void LeaderThread(URI controllerURI, String scope, String readerGroupName) {
        log.info("LeaderThread: BEGIN; this={}", System.identityHashCode(this));
        try {
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
                try (final ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName)) {
                    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
                    final String checkpointName = UUID.randomUUID().toString();
                    log.info("Calling initiateCheckpoint(checkpointName={})", checkpointName);
                    CompletableFuture<Checkpoint> checkpointFuture = readerGroup.initiateCheckpoint(checkpointName, scheduler);
                    log.info("Calling checkpointFuture.get()");
                    Checkpoint checkpoint = checkpointFuture.get();
                    log.info("checkpoint={}, positions={}", checkpoint, checkpoint.asImpl().getPositions());
                    scheduler.shutdown();
                    Thread.sleep(1000);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        log.info("LeaderThread: END");
    }

    void ReaderThread(URI controllerURI, String scope, String readerGroupName) {
        try {
            final String readerId = "readerId1";
            try (final ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI)) {
                try (final EventStreamReader<byte[]> reader = clientFactory.createReader(
                        readerId,
                        readerGroupName,
                        new ByteArraySerializer(),
                        ReaderConfig.builder().build())) {
                    for (; ; ) {
                        final EventRead<byte[]> eventRead = reader.readNextEvent(10000);
                        log.info("eventRead={}", eventRead);
                        if (eventRead.isCheckpoint()) {
                            log.info("Got checkpoint");
                        } else {
                            byte[] evt = eventRead.getEvent();
                            log.info("evt={}", AbstractPravegaProcessor.dumpByteArray(evt));
                            if (evt != null) {
                                if (evt.length == 1) {
                                    log.info("Got quit message");
                                    break;
                                }
                            }
                        }
//                        Thread.sleep(1000);
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
