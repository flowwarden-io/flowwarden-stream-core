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
/**
 * The MongoDB implementations of the FlowWarden state SPIs. Public API.
 *
 * <p>FlowWarden keeps three kinds of state, each behind one SPI of
 * {@code io.flowwarden.stream.spi} and each with one MongoDB implementation per
 * driver:</p>
 * <ul>
 *   <li>{@code checkpoint} — {@code CheckpointStore}: {@code _fw_checkpoints};</li>
 *   <li>{@code dlq} — {@code DlqStore}: {@code _fw_dlq} (or the configured collections);</li>
 *   <li>{@code lock} — {@code LockService}: {@code _fw_locks}.</li>
 * </ul>
 *
 * <p>The auto-configuration builds them on the default template of the
 * execution mode. Each bean is {@code @ConditionalOnMissingBean}: an application
 * whose state belongs in another database redeclares the three beans on the
 * template of that database, with the public constructors of these classes.</p>
 */
package io.flowwarden.stream.mongo;
