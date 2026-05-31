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
package io.flowwarden.stream.annotation;

import io.flowwarden.stream.OnHistoryLost;
import io.flowwarden.stream.StartPosition;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables automatic persistence of Change Stream resume tokens.
 *
 * <p>Place this annotation on a class that is also annotated with
 * {@link ChangeStream}. The framework will save the resume token after
 * every {@link #saveEveryN()} successfully processed event(s) and
 * resume from the last checkpoint on restart. A periodic timer
 * ({@link #saveIntervalSeconds()}) also saves the latest token as a
 * heartbeat, even when no events are arriving.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Checkpoint {

    /** Save a checkpoint every N successfully processed events. */
    int saveEveryN() default 1;

    /**
     * Periodic checkpoint interval in seconds.
     * The framework saves the latest resume token at this interval,
     * even if no events have been processed (heartbeat).
     * Set to {@code 0} to disable periodic saving.
     */
    int saveIntervalSeconds() default 5;

    /** Where to start consuming when the stream is (re)started. */
    StartPosition startPosition() default StartPosition.RESUME;

    /** Strategy to apply when the saved resume token has expired from the oplog. */
    OnHistoryLost onHistoryLost() default OnHistoryLost.FAIL;
}
