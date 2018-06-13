package org.apache.nifi.processors.pravega;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.nifi.components.*;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractPravegaProcessor extends AbstractSessionFactoryProcessor {

    static final Validator CONTROLLER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            try{
                final URI controllerURI = new URI(input);
            } catch (URISyntaxException e) {
                return new ValidationResult.Builder().subject(subject).valid(false).explanation("it is not valid URI syntax.").build();
            }
            return new ValidationResult.Builder().subject(subject).valid(true).build();
        }
    };

    static final PropertyDescriptor PROP_CONTROLLER = new PropertyDescriptor.Builder()
            .name("controller")
            .displayName("Controller URI")
            .description("The URI of the Pravega controller (e.g. tcp://localhost:9090).")
            .required(true)
            .addValidator(CONTROLLER_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_SCOPE = new PropertyDescriptor.Builder()
            .name("scope")
            .displayName("Scope")
            .description("The name of the Pravega scope.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_STREAM = new PropertyDescriptor.Builder()
            .name("stream")
            .displayName("Stream Name")
            .description("The name of the Pravega stream.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();ComponentLog logger;

    static final AllowableValue SCALE_TYPE_FIXED_NUM_SEGMENTS = new AllowableValue(
            "FIXED_NUM_SEGMENTS",
            "fixed number of segments",
            "");
    static final AllowableValue SCALE_TYPE_BY_RATE_IN_KBYTES_PER_SEC = new AllowableValue(
            "BY_RATE_IN_KBYTES_PER_SEC",
            "by rate in KB/sec",
            "scale up/down to maintain the specified rate per segment");
    static final AllowableValue SCALE_TYPE_BY_RATE_IN_EVENTS_PER_SEC = new AllowableValue(
            "BY_RATE_IN_EVENTS_PER_SEC",
            "by rate in events/sec",
            "");

    static final PropertyDescriptor PROP_SCALE_TYPE = new PropertyDescriptor.Builder()
            .name("scale.type")
            .displayName("Scale Type")
            .description("")
            .required(true)
            .allowableValues(SCALE_TYPE_FIXED_NUM_SEGMENTS, SCALE_TYPE_BY_RATE_IN_KBYTES_PER_SEC, SCALE_TYPE_BY_RATE_IN_EVENTS_PER_SEC)
            .defaultValue(SCALE_TYPE_FIXED_NUM_SEGMENTS.getValue())
            .build();

    static final PropertyDescriptor PROP_SCALE_TARGET_RATE = new PropertyDescriptor.Builder()
            .name("scale.target.rate")
            .displayName("Scale Target Rate")
            .description("")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    static final PropertyDescriptor PROP_SCALE_FACTOR = new PropertyDescriptor.Builder()
            .name("scale.factor")
            .displayName("Scale Target Rate")
            .description("")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("2")
            .build();

    static final PropertyDescriptor PROP_SCALE_MIN_NUM_SEGMENTS = new PropertyDescriptor.Builder()
            .name("scale.min.num.segments")
            .displayName("Minimum Number of Segments")
            .description("")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    static List<PropertyDescriptor> getAbstractPropertyDescriptors(){
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_CONTROLLER);
        descriptors.add(PROP_SCOPE);
        descriptors.add(PROP_STREAM);
        descriptors.add(PROP_SCALE_TYPE);
        descriptors.add(PROP_SCALE_TARGET_RATE);
        descriptors.add(PROP_SCALE_FACTOR);
        descriptors.add(PROP_SCALE_MIN_NUM_SEGMENTS);
        return descriptors;
    }

    public ClientConfig getClientConfig(final ProcessContext context) {
        try {
            final URI controllerURI = new URI(context.getProperty(PROP_CONTROLLER).getValue());
            final ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(controllerURI)
                    .build();
            return clientConfig;
        } catch (final URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public StreamConfiguration getStreamConfiguration(final ProcessContext context) {
        final ScalingPolicy.ScaleType scaleType = ScalingPolicy.ScaleType.valueOf(context.getProperty(PROP_SCALE_TYPE).getValue());
        final int targetRate = Integer.parseInt(context.getProperty(PROP_SCALE_TARGET_RATE).getValue());
        final int scaleFactor = Integer.parseInt(context.getProperty(PROP_SCALE_FACTOR).getValue());
        final int minNumSegments = Integer.parseInt(context.getProperty(PROP_SCALE_MIN_NUM_SEGMENTS).getValue());
        ScalingPolicy scalingPolicy;
        switch (scaleType) {
            case BY_RATE_IN_EVENTS_PER_SEC:
                scalingPolicy = ScalingPolicy.byEventRate(targetRate, scaleFactor, minNumSegments);
                break;
            case BY_RATE_IN_KBYTES_PER_SEC:
                scalingPolicy = ScalingPolicy.byDataRate(targetRate, scaleFactor, minNumSegments);
                break;
            case FIXED_NUM_SEGMENTS:
            default:
                scalingPolicy = ScalingPolicy.fixed(minNumSegments);
        }
        final StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .build();
        logger.debug("getStreamConfiguration: streamConfig={}", new Object[]{streamConfig});
        return streamConfig;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        System.out.println("AbstractPravegaProcessor: onTrigger: BEGIN");
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, sessionFactory, session);
            session.commit();
        } catch (final Throwable t) {
            session.rollback(true);
            System.out.println("AbstractPravegaProcessor: onTrigger: Exception: " + t.toString());
            throw t;
        }
        System.out.println("AbstractPravegaProcessor: onTrigger: END");
    }

    public abstract void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory, final ProcessSession session) throws ProcessException;

    public boolean isPrimaryNode() {
        // TODO: This is not working as expected when the last node of a cluster is stopped. It should return true but actually returns false.
        final NodeTypeProvider provider = getNodeTypeProvider();
        final boolean isClustered = provider.isClustered();
        final boolean isPrimary = provider.isPrimary();
        final boolean isPrimaryNode = (!isClustered) || isPrimary;
        logger.debug("isPrimaryNode: isClustered={}, isPrimary={}, isPrimaryNode={}",
                new Object[]{isClustered, isPrimary, isPrimaryNode});
        return isPrimaryNode;
    }

    /**
     * Builds transit URI for provenance event. The transit URI will be in the form of
     * tcp://localhost:9090/scope/stream.
     */
    static String buildTransitURI(String controller, String scope, String streamName) {
        return String.format("%s/%s/%s", controller, scope, streamName);
    }

    // Based on https://stackoverflow.com/a/13006907/5890553.
    public static String dumpByteArray(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 3);
        for (byte b: a)
            sb.append(String.format("%02x ", b));
        return sb.toString();
    }
}
