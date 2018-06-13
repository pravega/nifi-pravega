package org.apache.nifi.processors.pravega;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"Pravega", "Nautilus", "Get", "Ingest", "Ingress", "Receive", "Consume", "Subscribe", "Stream"})
@CapabilityDescription("Consumes events from Pravega.")
@WritesAttributes({
        @WritesAttribute(attribute = ConsumePravega.ATTR_EVENT_POINTER, description = "A string identifying the location of the event in the stream"),
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = Scope.CLUSTER, description =
        "This processor stores the most recent successful checkpoint in the cluster state to allow it to resume when restarting the processor or node."
        + "It also stores the Pravega reader group name to allow other readers to join the reader group. "
        + "The state must be cleared in order for any change in the set of streams to become effective.")
@SeeAlso({PublishPravega.class})
public class ConsumePravega extends AbstractPravegaProcessor {

    static final String ATTR_EVENT_POINTER = "pravega.event.pointer";

    static final AllowableValue STREAM_CUT_EARLIEST = new AllowableValue(
            "earliest",
            "earliest",
            "Start at the earliest available event (head)");
    static final AllowableValue STREAM_CUT_LATEST = new AllowableValue(
            "latest",
            "latest",
            "Start at the latest event (tail)");
    static final PropertyDescriptor PROP_STREAM_CUT_METHOD = new PropertyDescriptor.Builder()
            .name("stream.cut.method")
            .displayName("Start From")
            .description("If there is not a checkpoint saved in the state of this processor, then this specifies where to start from. "
                    + "You must clear the state for changes to this property to take effect.")
            .required(true)
            .allowableValues(STREAM_CUT_EARLIEST, STREAM_CUT_LATEST)
            .defaultValue(STREAM_CUT_LATEST.getValue())
            .build();

    static final PropertyDescriptor PROP_CHECKPOINT_PERIOD = new PropertyDescriptor.Builder()
            .name("checkpoint.period")
            .displayName("Checkpoint Period")
            .description("Checkpoints must be periodically performed to commit FlowFiles and save the position of each reader to the state. "
                    + "This property specifies the time interval between such checkpoints. "
                    + "Note that FlowFiles are not visible to the success relationship until a checkpoint occurs.")
            .required(true)
            .defaultValue("1 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_CHECKPOINT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("checkpoint.timeout")
            .displayName("Checkpoint Timeout")
            .description("Checkpoints that take longer than this time to complete (due to unavailable readers for instance) "
                    + "will be aborted and any uncommitted FlowFiles will be rolled back.")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_STOP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("stop.timeout")
            .displayName("Stop Timeout")
            .description("When stopping this processor, it will attempt to take a final checkpoint so that it can later start "
                    + "from exactly where it left off. "
                    + "However, if this graceful shutdown process takes longer than this duration, it will be aborted. "
                    + "In such a case, the most recent completed checkpoint will be used when restarting, "
                    + "possibly resulting in duplicate FlowFiles.")
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_MINIMUM_PROCESSING_TIME = new PropertyDescriptor.Builder()
            .name("minimum.processing.time")
            .displayName("Minimum Processing Time")
            .description("Whenever this processor is scheduled, it will run for at least this duration and then stop upon "
                    + "reaching a checkpoint.")
            .required(true)
            .defaultValue("100 ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Pravega.")
            .build();

    private volatile ConsumerPool consumerPool = null;

    static final List<PropertyDescriptor> DESCRIPTORS;
    static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> descriptors = getAbstractPropertyDescriptors();
        descriptors.add(PROP_STREAM_CUT_METHOD);
        descriptors.add(PROP_CHECKPOINT_PERIOD);
        descriptors.add(PROP_CHECKPOINT_TIMEOUT);
        descriptors.add(PROP_STOP_TIMEOUT);
        descriptors.add(PROP_MINIMUM_PROCESSING_TIME);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);
        RELATIONSHIPS = Collections.singleton(REL_SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        System.out.println("ConsumePravega.onUnscheduled: BEGIN");
        // AbstractSessionFactoryProcessor.updateScheduledFalse is annotated with @OnUnscheduled but we are unsure
        // if it was already called. Therefore, we ensure that it is called here.
        updateScheduledFalse();
        ConsumerPool pool = consumerPool;
        if (pool != null) {
            pool.gracefulShutdown(context);
        }
        System.out.println("ConsumePravega.onUnscheduled: END");
    }

    @OnStopped
    public void close(final ProcessContext context) {
        logger.info("ConsumePravega.close: BEGIN");
        System.out.println("ConsumePravega.close: BEGIN");
        ConsumerPool pool;
        synchronized (this) {
            pool = consumerPool;
            consumerPool = null;
        }
        if (pool != null) {
            pool.close();
        }
        logger.info("ConsumePravega.close: END");
        System.out.println("ConsumePravega.close: END");
    }

    private synchronized ConsumerPool getConsumerPool(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws Exception {
        ConsumerPool pool = consumerPool;
        if (pool != null) {
            return pool;
        }

        return consumerPool = createConsumerPool(context, sessionFactory, getLogger());
    }

    protected ConsumerPool createConsumerPool(final ProcessContext context, final ProcessSessionFactory sessionFactory, final ComponentLog log) throws Exception {
        final ClientConfig clientConfig = getClientConfig(context);
        final List<Stream> streams = getStreams(context);
        final StreamConfiguration streamConfig = getStreamConfiguration(context);
        final int maxConcurrentLeases = context.getMaxConcurrentTasks();
        final long checkpointPeriodMs = context.getProperty(PROP_CHECKPOINT_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS);
        final long checkpointTimeoutMs = context.getProperty(PROP_CHECKPOINT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final long gracefulShutdownTimeoutMs = context.getProperty(PROP_STOP_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final long minimumProcessingTimeMs = context.getProperty(PROP_MINIMUM_PROCESSING_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final String streamCutMethod = context.getProperty(PROP_STREAM_CUT_METHOD).getValue();
        return new ConsumerPool(
                log,
                context.getStateManager(),
                sessionFactory,
                this::isPrimaryNode,
                maxConcurrentLeases,
                checkpointPeriodMs,
                checkpointTimeoutMs,
                gracefulShutdownTimeoutMs,
                minimumProcessingTimeMs,
                clientConfig,
                streams,
                streamConfig,
                streamCutMethod,
                null,
                null);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory, final ProcessSession session) throws ProcessException {
        logger.debug("onTrigger: BEGIN");
        System.out.println("onTrigger: BEGIN");
        try {
            final ConsumerPool pool = getConsumerPool(context, sessionFactory);
            System.out.println("onTrigger: Return from getConsumerPool");
            if (pool == null) {
                context.yield();
            } else {
                try (final ConsumerLease lease = pool.obtainConsumer(session, context)) {
                    System.out.println("onTrigger: Return from obtainConsumer");
                    if (lease == null) {
                        context.yield();
                    } else {
                        try {
                            if (this.isScheduled()) {
                                System.out.println("onTrigger: Calling readEvents");
                                if (!lease.readEvents()) {
                                    context.yield();
                                }
                                System.out.println("onTrigger: Return from readEvents");
                            }
                        } catch (final Throwable t) {
                            logger.error("Exception while processing data from Pravega so will close the lease {} due to {}",
                                    new Object[]{lease, t}, t);
                            context.yield();
                        }
                    }
                }
            }
        } catch (final ProcessorNotReadyException e) {
            // This is an expected exception that occurs during startup of a non-primary node.
            logger.info("onTrigger: ProcessorNotReadyException", new Object[]{e});
            context.yield();
        } catch (final Exception e) {
            logger.info("onTrigger: Exception", new Object[]{e});
            System.out.println("onTrigger: Exception: " + e.toString());
            throw new RuntimeException(e);
        }
        logger.debug("onTrigger: END");
        System.out.println("onTrigger: END");
    }

}
