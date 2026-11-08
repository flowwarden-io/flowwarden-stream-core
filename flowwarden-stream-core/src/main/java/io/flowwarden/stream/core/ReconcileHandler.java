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
package io.flowwarden.stream.core;

import io.flowwarden.stream.HistoryLostContext;

/**
 * Functional interface for the application's reconciliation of a lost
 * history window, mirroring {@code @Reconcile} for contributed streams.
 *
 * <p>Same contract as the annotation: runs before the cursor opens and before
 * any checkpoint write, must be idempotent, and a throw stops the start with
 * {@link io.flowwarden.stream.ReconciliationFailedException}.</p>
 *
 * @see io.flowwarden.stream.annotation.Reconcile
 * @see io.flowwarden.stream.registration.StreamSpec.Builder#reconcile(ReconcileHandler)
 */
@FunctionalInterface
public interface ReconcileHandler {

    /**
     * Re-synchronizes the application's view over the lost window described
     * by {@code ctx}.
     *
     * @param ctx the bound of the lost window and the strategy in effect
     */
    void reconcile(HistoryLostContext ctx);
}
