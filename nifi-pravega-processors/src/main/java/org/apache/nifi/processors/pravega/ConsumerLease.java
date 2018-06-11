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
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.nifi.processors.pravega.ConsumePravega.REL_SUCCESS;
import static org.apache.nifi.processors.pravega.ConsumerPool.CHECKPOINT_NAME_FINAL_PREFIX;

/**
 * This class represents a lease to access a Pravega Reader object. The lease is
 * intended to be obtained from a ConsumerPool. The lease is closeable to allow
 * for the clean model of a try w/resources whereby non-exceptional cases mean
 * the lease will be returned to the pool for future use by others. A given
 * lease may only belong to a single thread a time.
 */
public abstract class ConsumerLease implements Closeable {

    protected final EventStreamReader<byte[]> reader;
    protected final String readerId;
    private final ComponentLog logger;
    private final RecordSetWriterFactory writerFactory;
    private final RecordReaderFactory readerFactory;
    private boolean poisoned = false;

    ConsumerLease(
            final EventStreamReader<byte[]> reader,
            final String readerId,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger) {
        this.reader = reader;
        this.readerId = readerId;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.logger = logger;
    }

    public String toString() {
        return String.format("ConsumerLease[readerId=%s]", readerId);
    }

    private EventRead<byte[]> nextEventToRead = null;

    /**
     * Reads events from the Pravega reader and creates FlowFiles.
     * This method will be called periodically by onTrigger.
     *
     * Note that it is possible to receive a false final checkpoint.
     * See ConsumerPool.gracefulShutdown().
     * Therefore, we should allow for normal events and checkpoints to occur even after
     * a final checkpoint.
     *
     * If this method has read and processed data up to a checkpoint, it will return successfully.
     * Otherwise, it will throw an exception.
     *
     * @return false if calling onTrigger should yield; otherwise true
     *
     */
    boolean readEvents() {
        logger.debug("readEvents: BEGIN: {}", new Object[]{this.toString()});
        System.out.println("readEvents: BEGIN: " + this);
        long eventCount = 0;
        final long startTime = System.currentTimeMillis();
        final long minStopTime = startTime + 100;
        final long timeoutTime = startTime + 10000;
        try {
            while (true) {
                EventRead<byte[]> eventRead = nextEventToRead;
                nextEventToRead = null;
                if (eventRead == null) {
                    final long readTimeoutTime = Math.max(0, timeoutTime - System.currentTimeMillis());
                    logger.debug("readEvents: {}:  Calling readNextEvent with readTimeoutTime={}", new Object[]{this, readTimeoutTime});
                    System.out.println("readEvents: " + this + ": Calling readNextEvent with readTimeoutTime=" + readTimeoutTime);
                    if (true) {
                        eventRead = reader.readNextEvent(readTimeoutTime);
                    } else {
                        ExecutorService executor = Executors.newCachedThreadPool();
                        try {
                            eventRead = executor.submit(() -> reader.readNextEvent(readTimeoutTime)).get(readTimeoutTime + 1000, TimeUnit.MILLISECONDS);
                        } finally {
                            executor.shutdown();
                        }
                    }
                    logger.debug("readEvents: eventRead={}", new Object[]{eventRead});
                    System.out.println("readEvents: " + this + ": eventRead=" + eventRead.toString());
                } else {
                    logger.debug("readEvents: (saved) eventRead={}", new Object[]{eventRead});
                    System.out.println("readEvents: " + this + ": (saved) eventRead=" + eventRead.toString());
                }
                if (eventRead.isCheckpoint()) {
                    // If a checkpoint was in the queue, it will be the first event returned.
                    // If this case, we want to continue to read until the 2nd checkpoint.
                    if (eventCount > 0) {
                        getProcessSession().commit();
                        final long transmissionMillis = System.currentTimeMillis() - startTime;
                        logger.info("Received and committed {} events in {} milliseconds from readerId {}.",
                                new Object[]{eventCount, transmissionMillis, readerId});
                    }
                    // Call readNextEvent to indicate to Pravega that we are done with the checkpoint.
                    // A non-timeout result will be stored and used at the next iteration;
                    nextEventToRead = reader.readNextEvent(0);
                    if (nextEventToRead.getEvent() == null && !nextEventToRead.isCheckpoint()) {
                        nextEventToRead = null;
                    }
                    boolean isFinalCheckpoint = eventRead.getCheckpointName().startsWith(CHECKPOINT_NAME_FINAL_PREFIX);
                    lastCheckpointIsFinal = isFinalCheckpoint;
                    if (isFinalCheckpoint) {
                        logger.debug("readEvents: got final checkpoint");
                        System.out.println("readEvents: END: " + this + ": Returning due to final checkpoint");
                        return false;
                    }
                    if (System.currentTimeMillis() > minStopTime) {
                        System.out.println("readEvents: END: " + this + ": Returning due to non-final checkpoint");
                        return true;
                    }
                    eventCount = 0;
                } else if (eventRead.getEvent() == null) {
                    // Sometimes readNextEvent returns no event even when a timeout has not occurred.
                    // If this occurs, we continue reading.
//                    if (System.currentTimeMillis() > timeoutTime) {
                        if (eventCount > 0) {
                            logger.warn("timeout waiting for event or checkpoint; session with {} events will be rolled back", new Object[]{eventCount});
                            throw new TimeoutException("timeout waiting for event or checkpoint");
                        } else {
                            logger.info("timeout waiting for event checkpoint; session has no events");
                            return false;
                        }
//                    }
                } else {
                    eventCount++;
                    processEvent(eventRead);
                }
            }
        }
        // We should throw a ProcessException if onTrigger should be called again immediately.
        // Otherwise, the processor will be administratively yielded for 10 seconds.
        catch (final ProcessException e) {
            System.out.println("readEvents: END: " + this + ": Exception: " + e.toString());
            throw e;
        } catch (final ReinitializationRequiredException e) {
            System.out.println("readEvents: END: " + this + ": Exception: " + e.toString());
            this.poison();
            throw new ProcessException(e);
        } catch (final TimeoutException | InterruptedException | ExecutionException e) {
            System.out.println("readEvents: END: " + this + ": Exception: " + e.toString());
            this.poison();
            throw new RuntimeException(e);
        } catch (final Throwable e) {
            System.out.println("readEvents: END: " + this + ": Exception: " + e.toString());
            this.poison();
            throw e;
        }
    }

    private boolean lastCheckpointIsFinal = false;

    void readEventsUntilFinalCheckpoint() {
        while (!lastCheckpointIsFinal) {
            readEvents();
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

    @Override
    public void close() {
    }

    public abstract ProcessSession getProcessSession();

    public abstract void yield();

    private void processEvent(final EventRead<byte[]> eventRead) {
        writeData(getProcessSession(), eventRead);
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

}
