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
package io.flowwarden.stream.registration;

/**
 * Contributes Change Stream definitions programmatically, as an alternative to
 * {@link io.flowwarden.stream.annotation.ChangeStream @ChangeStream}-annotated classes —
 * for streams whose configuration comes from outside the JVM (a YAML file, a database, a
 * feature-flag service, ...).
 *
 * <p>Implementations are discovered as Spring beans. Every {@code contribute} call across
 * every registered contributor runs once, at application <strong>bootstrap</strong> — after
 * all singleton beans (including {@code @ChangeStream}-annotated ones) have been created,
 * but before the stream managers start reading the stream catalog. There is currently no
 * support for adding streams to an already-running instance; a contributed definition is
 * fixed for the lifetime of the application context.</p>
 *
 * <pre>{@code
 * @Component
 * class YamlStreamContributor implements StreamDefinitionContributor {
 *     public void contribute(StreamRegistration registration) {
 *         registration.stream("order-stream", Order.class)
 *                 .collection("orders")
 *                 .checkpoint(CheckpointSpec.defaults())
 *                 .onInsert((order, ctx) -> log.info("New order: {}", order.getId()));
 *     }
 * }
 * }</pre>
 *
 * <p>For the capabilities {@link StreamSpec} covers (see its Javadoc for what's out of
 * scope), a contributed stream goes through the same validation and the same defaults as an
 * annotated one — checkpoint/retry/DLQ bounds, handler mode match, and so on. A duplicate
 * stream name — whether it collides with an annotated stream or another contributed one —
 * fails application startup either way.</p>
 */
@FunctionalInterface
public interface StreamDefinitionContributor {

    /**
     * Declares one or more stream specifications on the given registration.
     *
     * @param registration collects the {@link StreamSpec} builders declared by this contributor
     */
    void contribute(StreamRegistration registration);
}
