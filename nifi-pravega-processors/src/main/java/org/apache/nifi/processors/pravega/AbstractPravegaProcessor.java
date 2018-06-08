package org.apache.nifi.processors.pravega;

import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractPravegaProcessor extends AbstractProcessor {
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
            .displayName("Pravega Controller URI")
            .description("The URI of the Pravega controller (e.g. tcp://localhost:9090).")
            .required(true)
            .addValidator(CONTROLLER_VALIDATOR)
            .build();
    static final PropertyDescriptor PROP_SCOPE = new PropertyDescriptor.Builder()
            .name("scope")
            .displayName("Pravega Scope")
            .description("The name of the Pravega scope.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    static final PropertyDescriptor PROP_STREAM = new PropertyDescriptor.Builder()
            .name("stream")
            .displayName("Pravega Stream")
            .description("The name of the Pravega stream.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();ComponentLog logger;

    static List<PropertyDescriptor> getAbstractPropertyDescriptors(){
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_CONTROLLER);
        descriptors.add(PROP_SCOPE);
        descriptors.add(PROP_STREAM);
        return descriptors;
    }

    private volatile boolean primaryNode = false;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
//        final boolean isClustered = context.getNodeTypeProvider().isClustered();
//        final boolean isPrimary = context.getNodeTypeProvider().isPrimary();
//        logger.debug("init: this={}, isClustered={}, isPrimary={}",
//                new Object[]{System.identityHashCode(this), isClustered, isPrimary});
//        primaryNode = (!isClustered) || isPrimary;
//        logger.debug("init: this={}, primaryNode={}",
//                new Object[]{System.identityHashCode(this), primaryNode});
    }

//    @OnPrimaryNodeStateChange
//    public void onPrimaryNodeStateChange(PrimaryNodeState state) {
//        logger.info("onPrimaryNodeStateChange: state={}", new Object[]{state});
//        primaryNode = state == PrimaryNodeState.ELECTED_PRIMARY_NODE;
//    }

    public boolean isPrimaryNode() {
        final boolean isPrimary = getNodeTypeProvider().isPrimary();
        logger.info("getNodeTypeProvider().isPrimary()={}", new Object[]{isPrimary});
        return isPrimary;
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
