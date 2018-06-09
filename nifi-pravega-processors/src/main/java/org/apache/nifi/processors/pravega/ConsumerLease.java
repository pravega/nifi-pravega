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
import java.util.concurrent.TimeUnit;

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

    private final EventStreamReader<byte[]> pravegaReader;
    private final String readerId;
    private final ComponentLog logger;
    private final RecordSetWriterFactory writerFactory;
    private final RecordReaderFactory readerFactory;
    private boolean poisoned = false;

    ConsumerLease(
            final EventStreamReader<byte[]> pravegaReader,
            final String readerId,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger) {
        this.pravegaReader = pravegaReader;
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
     * @return false if calling onTrigger should yield; otherwise true
     *
     */
    boolean readEvents() {
        logger.debug("readEvents: {}", new Object[]{this.toString()});
        long eventCount = 0;
        final long startTime = System.currentTimeMillis();
        final long stopTime = startTime + 100;
        try {
            while (true) {
                final long READER_TIMEOUT_MS = 10000;
                EventRead<byte[]> eventRead = nextEventToRead;
                nextEventToRead = null;
                if (nextEventToRead == null) {
                    eventRead = pravegaReader.readNextEvent(READER_TIMEOUT_MS);
                }
                logger.debug("readEvents: eventRead={}", new Object[]{eventRead});
                if (eventRead.isCheckpoint()) {
                    // If a checkpoint was in the queue, it will be the first event returned.
                    // If this case, we want to continue to read until the 2nd checkpoint.
                    if (eventCount > 0) {
                        getProcessSession().commit();
                        final long transmissionMillis = System.currentTimeMillis() - startTime;
                        logger.info("Received and committed {} events in {} milliseconds from readerId {}.",
                                new Object[]{eventCount, transmissionMillis, readerId});
                    }
                    // TODO: call readNextEvent to indicate that we are done with the checkpoint; need to save result and use it later.
                    // Call readNextEvent to indicate to Pravega that we are done with the checkpoint.
                    // The result will be stored and used at the next iteration;
                    nextEventToRead = pravegaReader.readNextEvent(0);
                    boolean isFinalCheckpoint = eventRead.getCheckpointName().startsWith(CHECKPOINT_NAME_FINAL_PREFIX);
                    if (isFinalCheckpoint) {
                        logger.debug("readEvents: got final checkpoint");
                        // Ensure that this lease (reader) does not get reused.
                        this.poison();
                        return false;
                    }
                    if (System.currentTimeMillis() > stopTime) {
                        return true;
                    }
                    eventCount = 0;
                } else if (eventRead.getEvent() == null) {
                    if (eventCount > 0) {
                        logger.warn("timeout waiting for checkpoint; session with {} events will be rolled back", new Object[]{eventCount});
                    } else {
                        logger.info("timeout waiting for checkpoint; session will be rolled back");
                    }
                    return true;
                } else {
                    eventCount++;
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
