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

import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/**
 * Translates {@link FlowWardenConfigurationException} into a Spring Boot
 * "APPLICATION FAILED TO START" message with description and corrective action.
 */
public class FlowWardenFailureAnalyzer extends AbstractFailureAnalyzer<FlowWardenConfigurationException> {

    @Override
    protected FailureAnalysis analyze(Throwable rootFailure, FlowWardenConfigurationException cause) {
        return new FailureAnalysis(cause.getDescription(), cause.getAction(), cause);
    }
}
