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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares that the application uses FlowWarden. Place on a {@code @Configuration}
 * or {@code @SpringBootApplication} class.
 *
 * <p>The auto-configuration activates from the classpath; this annotation is the
 * declarative entry point and carries the one application-wide switch,
 * {@link #enabled()}. At startup, FlowWarden discovers beans annotated with
 * {@link ChangeStream} via standard Spring component scanning and registers them
 * in the stream registry.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EnableFlowWarden {

    /**
     * Whether registered streams start automatically on
     * {@code ApplicationReadyEvent}. {@code false} is a maintenance switch:
     * every stream is discovered and validated as usual but stays stopped,
     * and can still be started explicitly through
     * {@code FlowWardenStreamManager.startStream(name)} or the actuator
     * endpoint. Per-stream control stays on {@link ChangeStream#enabled()}
     * and {@link ChangeStream#autoStart()}.
     */
    boolean enabled() default true;
}
