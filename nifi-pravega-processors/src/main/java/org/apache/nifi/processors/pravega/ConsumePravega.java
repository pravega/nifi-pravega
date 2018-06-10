package org.apache.nifi.processors.pravega;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"Pravega", "Nautilus", "Get", "Ingest", "Ingress", "Receive", "Consume", "Subscribe", "Stream"})
@CapabilityDescription("Consumes events from Pravega.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = Scope.CLUSTER, description = "TODO After performing a fetching from HBase, stores a timestamp of the last-modified cell that was found. In addition, it stores the ID of the row(s) "
        + "and the value of each cell that has that timestamp as its modification date. This is stored across the cluster and allows the next fetch to avoid duplicating data, even if this Processor is "
        + "run on Primary Node only and the Primary Node changes.")
@SeeAlso({PublishPravega.class})
public class ConsumePravega extends AbstractPravegaProcessor {

//    static final AllowableValue OFFSET_EARLIEST = new AllowableValue("earliest", "earliest", "Automatically reset the offset to the earliest offset");
//
//    static final AllowableValue OFFSET_LATEST = new AllowableValue("latest", "latest", "Automatically reset the offset to the latest offset");
//
//    static final AllowableValue OFFSET_NONE = new AllowableValue("none", "none", "Throw exception to the consumer if no previous offset is found for the consumer's group");
//
//    static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
//            .name(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
//            .displayName("Offset Reset")
//            .description("Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any "
//                    + "more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.")
//            .required(true)
//            .allowableValues(OFFSET_EARLIEST, OFFSET_LATEST, OFFSET_NONE)
//            .defaultValue(OFFSET_LATEST.getValue())
//            .build();

    static final PropertyDescriptor MAX_UNCOMMITTED_TIME = new PropertyDescriptor.Builder()
            .name("max-uncommit-offset-wait")
            .displayName("Max Uncommitted Time")
            .description("Specifies the maximum amount of time allowed to pass before offsets must be committed. "
                    + "This value impacts how often offsets will be committed.  Committing offsets less often increases "
                    + "throughput but also increases the window of potential data duplication in the event of a rebalance "
                    + "or JVM restart between commits.")
            .required(false)
            .defaultValue("1 secs")
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
        descriptors.add(MAX_UNCOMMITTED_TIME);
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
        ConsumerPool pool = consumerPool;
        if (pool != null) {
            pool.gracefulShutdown(context);
//            close(context);
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

//    @OnShutdown
//    public void onShutdown() {
//        isPrimaryNode();
//        logger.info("onShutdown");
//        System.out.println("onShutdown");
//    }

    private synchronized ConsumerPool getConsumerPool(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws Exception {
        ConsumerPool pool = consumerPool;
        if (pool != null) {
            return pool;
        }

        return consumerPool = createConsumerPool(context, sessionFactory, getLogger());
    }

    protected ConsumerPool createConsumerPool(final ProcessContext context, final ProcessSessionFactory sessionFactory, final ComponentLog log) throws Exception {
        final int maxLeases = context.getMaxConcurrentTasks();
        logger.debug("createConsumerPool: maxLeases={}", new Object[]{maxLeases});
        final long maxUncommittedTime = context.getProperty(MAX_UNCOMMITTED_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final URI controllerURI = new URI(context.getProperty(PROP_CONTROLLER).getValue());
        final String scope = context.getProperty(PROP_SCOPE).getValue();
        final String streamName = context.getProperty(PROP_STREAM).getValue();
        final StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        List<String> streamNames = new ArrayList<>();
        streamNames.add(streamName);
        return new ConsumerPool(
                log,
                context.getStateManager(),
                sessionFactory,
                this::isPrimaryNode,
                maxLeases,
                maxUncommittedTime,
                controllerURI,
                scope,
                streamNames,
                streamConfig,
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
            logger.info("onTrigger: ProcessorNotReadyException: {}", new Object[]{e});
            context.yield();
        } catch (final Exception e) {
            System.out.println("onTrigger: Exception: " + e.toString());
            throw new RuntimeException(e);
        }
        logger.debug("onTrigger: END");
        System.out.println("onTrigger: END");
    }

}
