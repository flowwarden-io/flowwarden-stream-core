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
 * Service Provider Interface (SPI) for FlowWarden metrics collection.
 *
 * <p>The Core ships with a no-op implementation. The FlowWarden Reporter
 * (or any custom provider) implements {@link io.flowwarden.stream.spi.StreamMetricsProvider}
 * and registers it via {@link io.flowwarden.stream.FlowWardenMetrics#setProvider}.</p>
 */
package io.flowwarden.stream.spi;
