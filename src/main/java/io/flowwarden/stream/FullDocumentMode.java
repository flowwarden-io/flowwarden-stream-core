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
package io.flowwarden.stream;

/**
 * Controls inclusion of the full document in change events.
 * Maps to MongoDB {@code FullDocument} option.
 */
public enum FullDocumentMode {

    /** Server default behaviour. */
    DEFAULT,

    /** Lookup the full document for update events. */
    UPDATE_LOOKUP,

    /** Include full document when available, {@code null} otherwise (MongoDB 6.0+). */
    WHEN_AVAILABLE,

    /** Require full document — error if not available (MongoDB 6.0+). */
    REQUIRED
}
