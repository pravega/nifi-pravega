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
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.keycloak.client.PravegaKeycloakCredentials;
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

    static final String STREAM_NAME_DESCRIPTION = "\nFor consumers, the state must be cleared in order for any change in the set of streams to become effective.";

    static final PropertyDescriptor PROP_SCOPE = new PropertyDescriptor.Builder()
            .name("scope")
            .displayName("Scope")
            .description("The name of the default Pravega scope. This will be used for any unqualified stream names."
                    + STREAM_NAME_DESCRIPTION)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_STREAM = new PropertyDescriptor.Builder()
            .name("stream")
            .displayName("Stream Name")
            .description("The name of the Pravega stream. Stream names may be contain a scope in the format 'scope/stream'. "
                    + "Consumers may specify a comma-separated list."
                    + STREAM_NAME_DESCRIPTION)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final AllowableValue SCALE_TYPE_FIXED_NUM_SEGMENTS = new AllowableValue(
            "FIXED_NUM_SEGMENTS",
            "fixed number of segments",
            "");
    static final AllowableValue SCALE_TYPE_BY_RATE_IN_KBYTES_PER_SEC = new AllowableValue(
            "BY_RATE_IN_KBYTES_PER_SEC",
            "by data rate in KB/sec",
            "Scale up/down the number of segments to maintain the specified data rate per segment. See 'Scale Target Rate'.");
    static final AllowableValue SCALE_TYPE_BY_RATE_IN_EVENTS_PER_SEC = new AllowableValue(
            "BY_RATE_IN_EVENTS_PER_SEC",
            "by event rate in events/sec",
            "Scale up/down the number of segments to maintain the specified event rate per segment. See 'Scale Target Rate'.");

    static final String STREAM_CONFIG_DESCRIPTION = "\nThis setting will be ignored if the stream already exists.";

    static final PropertyDescriptor PROP_SCALE_TYPE = new PropertyDescriptor.Builder()
            .name("scale.type")
            .displayName("Scale Type")
            .description("Controls the scaling policy."
                    + STREAM_CONFIG_DESCRIPTION)
            .required(true)
            .allowableValues(SCALE_TYPE_FIXED_NUM_SEGMENTS, SCALE_TYPE_BY_RATE_IN_KBYTES_PER_SEC, SCALE_TYPE_BY_RATE_IN_EVENTS_PER_SEC)
            .defaultValue(SCALE_TYPE_FIXED_NUM_SEGMENTS.getValue())
            .build();

    static final PropertyDescriptor PROP_SCALE_TARGET_RATE = new PropertyDescriptor.Builder()
            .name("scale.target.rate")
            .displayName("Scale Target Rate")
            .description("Units are defined in the Scale Type attribute. "
                    + "Unused for 'fixed number of segments'."
                    + STREAM_CONFIG_DESCRIPTION)
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    static final PropertyDescriptor PROP_SCALE_FACTOR = new PropertyDescriptor.Builder()
            .name("scale.factor")
            .displayName("Scale Factor")
            .description("Segments exceeding the specified rate will be split into this many segments. "
                    + "Also, this many adjacent segments can be merged together if rates are lower than the specified rate. "
                    + "Unused for 'fixed number of segments'."
                    + STREAM_CONFIG_DESCRIPTION)
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("2")
            .build();

    static final PropertyDescriptor PROP_SCALE_MIN_NUM_SEGMENTS = new PropertyDescriptor.Builder()
            .name("scale.min.num.segments")
            .displayName("Minimum Number of Segments")
            .description("The number of segments will never be below this."
                    + STREAM_CONFIG_DESCRIPTION)
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    static final PropertyDescriptor PROP_KEYCLOAK_JSON = new PropertyDescriptor.Builder()
            .name("keycloakJson")
            .displayName("Keycloak Json")
            .description("The Keycloak Json.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_CREATE_SCOPE = new PropertyDescriptor.Builder()
            .name("PROP_CREATE_SCOPE")
            .displayName("Create Scope (true/false)")
            .description("Create Scope or not.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
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
        descriptors.add(PROP_KEYCLOAK_JSON);
        descriptors.add(PROP_CREATE_SCOPE);
        return descriptors;
    }

    ComponentLog logger;

    public ClientConfig getClientConfig(final ProcessContext context) {
        try {
            ClientConfig clientConfig = null;
            boolean createScope = new Boolean(context.getProperty(PROP_CREATE_SCOPE).getValue()).booleanValue();
            final URI controllerURI = new URI(context.getProperty(PROP_CONTROLLER).getValue());
            final ClientConfig.ClientConfigBuilder clientBuilder = ClientConfig.builder().controllerURI(controllerURI);
            if(createScope)
            {
                clientConfig = ClientConfig.builder()
                        .controllerURI(controllerURI)
                        .build();
            }
            else if(!createScope && (context.getProperty(PROP_KEYCLOAK_JSON).getValue() != null))
            {
                clientBuilder.credentials(new PravegaKeycloakCredentialsFromString(context.getProperty(PROP_KEYCLOAK_JSON).getValue()));
                clientConfig = clientBuilder.build();
            } else if(context.getProperty(PROP_KEYCLOAK_JSON).getValue() == null || context.getProperty(PROP_KEYCLOAK_JSON).getValue().isEmpty())
            {
                logger.error("Keycloak json property not set");
                throw new RuntimeException("Keycloak json property not set");
            }
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

    /**
     * Resolves the given stream name.
     *
     * The scope name is resolved in the following order:
     * 1. from the stream name (if fully-qualified)
     * 2. from the default scope
     *
     * @param streamSpec a qualified or unqualified stream name
     * @return a fully-qualified stream name
     * @throws IllegalStateException if an unqualified stream name is supplied but the scope is not configured.
     */
    public static Stream resolveStream(String defaultScope, String streamSpec) {
        if (streamSpec == null) {
            throw new NullPointerException("streamSpec");
        }
        String[] split = streamSpec.split("/", 2);
        if (split.length == 1) {
            // unqualified
            if (defaultScope == null) {
                throw new IllegalStateException("The default scope is not configured.");
            }
            return Stream.of(defaultScope, split[0]);
        } else {
            // qualified
            assert split.length == 2;
            return Stream.of(split[0], split[1]);
        }
    }

    public List<Stream> getStreams(final ProcessContext context) {
        final String defaultScope = context.getProperty(PROP_SCOPE).getValue();
        final String streamSpecs = context.getProperty(PROP_STREAM).getValue();
        final String[] streamSpecsList = streamSpecs.split(",");
        final List<Stream> streams = new ArrayList<>();
        for (final String streamName : streamSpecsList) {
            final String trimmedName = streamName.trim();
            if (!trimmedName.isEmpty()) {
                final Stream stream = resolveStream(defaultScope, streamName);
                streams.add(stream);
            }
        }
        if (streams.isEmpty()) {
            throw new IllegalArgumentException("you must specify at least one stream");
        }
        logger.debug("getStreams: streams={}", new Object[]{streams});
        return streams;
    }

    public Stream getStream(final ProcessContext context) {
        final List<Stream> streams = getStreams(context);
        if (streams.size() != 1) {
            throw new IllegalArgumentException("you must specify exactly one stream");
        }
        return streams.get(0);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, sessionFactory, session);
            session.commit();
        } catch (final Throwable t) {
            session.rollback(true);
            throw t;
        }
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
