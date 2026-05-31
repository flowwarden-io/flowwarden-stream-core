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
package io.flowwarden.stream.internal.discovery;

import io.flowwarden.stream.DeploymentMode;
import io.flowwarden.stream.FullDocumentBeforeChangeMode;
import io.flowwarden.stream.FullDocumentMode;
import io.flowwarden.stream.annotation.ChangeStream;
import org.bson.Document;

/**
 * Holds the key configuration extracted from a {@link ChangeStream} annotation
 * (or built programmatically via a future builder API).
 *
 * <p>This class is internal and not part of the public API.</p>
 *
 * @param enabled                    whether the stream is enabled
 * @param autoStart                  whether the stream should start automatically
 * @param documentType               the Java type for document deserialization
 * @param mongoTemplateRef           the name of the MongoTemplate bean to use (empty = default)
 * @param fullDocument               full document inclusion mode
 * @param fullDocumentBeforeChange   pre-image inclusion mode (MongoDB 6.0+)
 * @param deploymentMode             multi-instance deployment strategy
 */
public record StreamConfig(
        boolean enabled,
        boolean autoStart,
        Class<?> documentType,
        String mongoTemplateRef,
        FullDocumentMode fullDocument,
        FullDocumentBeforeChangeMode fullDocumentBeforeChange,
        DeploymentMode deploymentMode
) {

    /**
     * Creates a {@link StreamConfig} from a {@link ChangeStream} annotation.
     *
     * @param ann the annotation instance
     * @return a new config record
     */
    public static StreamConfig fromAnnotation(ChangeStream ann) {
        return new StreamConfig(ann.enabled(), ann.autoStart(),
                ann.documentType(), ann.mongoTemplateRef(),
                ann.fullDocument(), ann.fullDocumentBeforeChange(),
                ann.deploymentMode());
    }
}
