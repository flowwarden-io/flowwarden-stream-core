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
package io.flowwarden.stream.spi;

/**
 * Reason a Change Stream stopped, as reported via
 * {@link StreamMetricsProvider#onStreamStopped(String, StopReason, Throwable)}.
 *
 * <p>Used by downstream metrics consumers to distinguish operator-initiated
 * stops from silent failures the operator never asked for.</p>
 */
public enum StopReason {

    /**
     * The stream was stopped intentionally — via an explicit
     * {@code stopStream()} call, a JVM shutdown hook, or loss of leadership
     * under {@code DeploymentMode.SINGLE_LEADER}.
     *
     * <p>The accompanying {@code cause} argument is always {@code null}.</p>
     */
    GRACEFUL,

    /**
     * The stream died on an uncaught {@link Throwable}: the imperative
     * {@code MessageListenerContainer} thread was killed, or the reactive
     * pipeline terminated on an unexpected {@code onError} / {@code onComplete}
     * signal. Without this signal, the console would keep reporting the
     * stream as {@code RUNNING} indefinitely.
     *
     * <p>The accompanying {@code cause} argument carries the {@code Throwable}
     * that escaped, or {@code null} if it could not be captured (rare).</p>
     */
    CRASHED
}
