/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.flowwarden.stream.autoconfigure;

/**
 * Thrown when FlowWarden detects an invalid configuration at startup.
 * Carries a description and suggested action for the {@link FlowWardenFailureAnalyzer}.
 */
public class FlowWardenConfigurationException extends RuntimeException {

    private final String description;
    private final String action;

    public FlowWardenConfigurationException(String description, String action) {
        super(description);
        this.description = description;
        this.action = action;
    }

    static final String MISSING_MONGO_TEMPLATE_DESCRIPTION =
            "FlowWarden is configured in IMPERATIVE mode but MongoTemplate is not available.";
    static final String MISSING_MONGO_TEMPLATE_ACTION =
            "Either:\n"
                    + "1. Add 'spring-boot-starter-data-mongodb' to your dependencies\n"
                    + "2. Change 'flowwarden.default-mode' to REACTIVE in your configuration";
    static final String MISSING_REACTIVE_MONGO_TEMPLATE_DESCRIPTION =
            "FlowWarden is configured in REACTIVE mode but ReactiveMongoTemplate is not available.";
    static final String MISSING_REACTIVE_MONGO_TEMPLATE_ACTION =
            "Either:\n"
                    + "1. Add 'spring-boot-starter-data-mongodb-reactive' to your dependencies\n"
                    + "2. Change 'flowwarden.default-mode' to IMPERATIVE in your configuration";

    /** IMPERATIVE mode configured, no {@code MongoTemplate} to run it on. */
    public static FlowWardenConfigurationException missingMongoTemplate() {
        return new FlowWardenConfigurationException(
                MISSING_MONGO_TEMPLATE_DESCRIPTION, MISSING_MONGO_TEMPLATE_ACTION);
    }

    /** REACTIVE mode configured, no {@code ReactiveMongoTemplate} to run it on. */
    public static FlowWardenConfigurationException missingReactiveMongoTemplate() {
        return new FlowWardenConfigurationException(
                MISSING_REACTIVE_MONGO_TEMPLATE_DESCRIPTION, MISSING_REACTIVE_MONGO_TEMPLATE_ACTION);
    }

    public String getDescription() {
        return description;
    }

    public String getAction() {
        return action;
    }
}
