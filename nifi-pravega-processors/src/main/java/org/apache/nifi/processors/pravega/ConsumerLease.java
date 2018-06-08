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

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReinitializationRequiredException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.pravega.ConsumePravega.REL_SUCCESS;
import static org.apache.nifi.processors.pravega.ConsumerPool.CHECKPOINT_NAME_QUIT_PREFIX;

/**
 * This class represents a lease to access a Pravega Reader object. The lease is
 * intended to be obtained from a ConsumerPool. The lease is closeable to allow
 * for the clean model of a try w/resources whereby non-exceptional cases mean
 * the lease will be returned to the pool for future use by others. A given
 * lease may only belong to a single thread a time.
 */
public abstract class ConsumerLease implements Closeable {

    private final long maxWaitMillis;
    private final EventStreamReader<byte[]> pravegaReader;
    private final ComponentLog logger;
    private final RecordSetWriterFactory writerFactory;
    private final RecordReaderFactory readerFactory;
    private boolean poisoned = false;
    private long leaseStartNanos = -1;
//    private boolean lastPollEmpty = false;
//    private int totalMessages = 0;
//    private boolean gotFinalCheckpoint = false;

    ConsumerLease(
            final long maxWaitMillis,
            final EventStreamReader<byte[]> pravegaReader,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger) {
        this.maxWaitMillis = maxWaitMillis;
        this.pravegaReader = pravegaReader;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.logger = logger;
    }

    /**
     * clears out internal state elements excluding session and consumer as
     * those are managed by the pool itself
     */
    private void resetInternalState() {
        leaseStartNanos = -1;
//        lastPollEmpty = false;
//        totalMessages = 0;
    }

    boolean gotQuitSignal = false;

    /**
     * Executes readNextEvent's on the underlying Pravega Reader and creates any new
     * flowfiles necessary.
     *
     * @return false if calling onTrigger should yield; otherwise true
     *
     */
    boolean readEventsUntilCheckpoint() {
        if (gotQuitSignal) {
            return false;
        }
        try {
            while (true) {
                final long READER_TIMEOUT_MS = 10000;
                final EventRead<byte[]> eventRead = pravegaReader.readNextEvent(READER_TIMEOUT_MS);
                if (eventRead.isCheckpoint()) {
                    // If this is a QUIT checkpoint, we rollback and exit.
                    boolean isQuitSignal = eventRead.getCheckpointName().startsWith(CHECKPOINT_NAME_QUIT_PREFIX);
                    if (isQuitSignal) {
                        gotQuitSignal = true;
                        logger.info("readEventsUntilCheckpoint: got QUIT signal");
                        // Call readNextEvent to indicate to Pravega that we are done with the checkpoint.
                        // The result will be discarded;
                        pravegaReader.readNextEvent(0);
                        // Ensure that this reader does not get reused.
                        this.poison();
                        return false;
                    } else {
                        getProcessSession().commit();
                        return true;
                    }
                } else if (eventRead.getEvent() == null) {
                    // Timeout occurred.
                    // We do not expect this to happen until the processor is stopping and periodic checkpoints have stopped.
                    this.poison();
                    throw new ProcessException("readNextEvent timed out; processed events will be rolled back");
                } else {
                    processEvent(eventRead);
                }
            }
        }
        catch (final ReinitializationRequiredException e) {
            // This reader must be closed so that a new one can be created.
            this.poison();
            throw new ProcessException(e);
        } catch (final ProcessException pe) {
            throw pe;
        } catch (final Throwable t) {
            this.poison();
            throw t;
        }
    }

    /**
     * Indicates that the underlying session and consumer should be immediately
     * considered invalid. Once closed the session will be rolled back and the
     * pool should destroy the underlying consumer. This is useful if due to
     * external reasons, such as the processor no longer being scheduled, this
     * lease should be terminated immediately.
     */
    private void poison() {
        poisoned = true;
    }

    /**
     * @return true if this lease has been poisoned; false otherwise
     */
    boolean isPoisoned() {
        return poisoned;
    }

    /**
     * Abstract method that is intended to be extended by the pool that created
     * this ConsumerLease object. It should ensure that the session given to
     * create this session is rolled back and that the underlying Pravega reader
     * is either returned to the pool for continued use or destroyed if this
     * lease has been poisoned. It can only be called once. Calling it more than
     * once can result in undefined and non threadsafe behavior.
     */
    @Override
    public void close() {
        resetInternalState();
    }

    public abstract ProcessSession getProcessSession();

    public abstract void yield();

    private void processEvent(final EventRead<byte[]> eventRead) {
        writeData(getProcessSession(), eventRead);
//        totalMessages++;
    }

    private void writeData(final ProcessSession session, final EventRead<byte[]> eventRead) {
        logger.debug("writeData: size={}, eventRead={}", new Object[]{eventRead.getEvent().length, eventRead});
        FlowFile flowFile = session.create();
        final byte[] value = eventRead.getEvent();
        if (value != null) {
            flowFile = session.write(flowFile, out -> {
                out.write(value);
            });
        }
        flowFile = session.putAllAttributes(flowFile, getAttributes(eventRead));
        // TODO: set transitUri
        final String transitUri = "TODO transitUri";
        session.getProvenanceReporter().receive(flowFile, transitUri);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private Map<String, String> getAttributes(final EventRead<byte[]> eventRead) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("pravega.event", eventRead.toString());
        attributes.put("pravega.event.getEventPointer", eventRead.getEventPointer().toString());
        return attributes;
    }

    // TODO: is below needed?
//    private void rollback(final TopicPartition topicPartition) {
//        try {
//            OffsetAndMetadata offsetAndMetadata = uncommittedOffsetsMap.get(topicPartition);
//            if (offsetAndMetadata == null) {
//                offsetAndMetadata = kafkaConsumer.committed(topicPartition);
//            }
//
//            final long offset = offsetAndMetadata == null ? 0L : offsetAndMetadata.offset();
//            kafkaConsumer.seek(topicPartition, offset);
//        } catch (final Exception rollbackException) {
//            logger.warn("Attempted to rollback Kafka message offset but was unable to do so", rollbackException);
//        }
//    }

}
