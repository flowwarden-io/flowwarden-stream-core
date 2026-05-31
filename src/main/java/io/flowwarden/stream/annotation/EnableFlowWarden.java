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
 * Activates the FlowWarden framework. Place on a {@code @Configuration}
 * or {@code @SpringBootApplication} class.
 *
 * <p>At startup, FlowWarden discovers beans annotated with {@link ChangeStream}
 * via standard Spring component scanning and registers them in the stream registry.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EnableFlowWarden {

    /** Property prefix for configuration. */
    String propertyPrefix() default "flowwarden.stream";

    /** Globally enable/disable all streams. */
    boolean enabled() default true;
}
