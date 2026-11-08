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
 * Declares the application's reconciliation of a lost history window, run
 * before the stream resumes after a history loss.
 *
 * <p>When both saved positions of a stream have aged out of the oplog
 * (resume cascade level 3) and the {@link Checkpoint#onHistoryLost()}
 * strategy is {@link io.flowwarden.stream.OnHistoryLost#RESUME_FROM_NOW
 * RESUME_FROM_NOW} or {@link io.flowwarden.stream.OnHistoryLost#RESUME_FROM_OPLOG_START
 * RESUME_FROM_OPLOG_START}, MongoDB has discarded the events of a window the
 * library can bound but not replay. A <em>derived view</em> — a search model,
 * a cache, a projection aggregated from several collections — is wrong over
 * that window until the application re-synchronizes it. The annotated method
 * is where that happens:</p>
 *
 * <pre>{@code
 * @ChangeStream(collection = "dossiers")
 * @Checkpoint(onHistoryLost = OnHistoryLost.RESUME_FROM_OPLOG_START)
 * public class DossierSearchProjection {
 *
 *     @OnInsert @OnUpdate
 *     public void project(Dossier d, ChangeStreamContext<Dossier> ctx) { ... }
 *
 *     @Reconcile
 *     public void rebuild(HistoryLostContext ctx) {
 *         Instant since = ctx.lostWindowStart().orElse(Instant.EPOCH);
 *         searchModel.resyncDossiersTouchedSince(since);
 *     }
 * }
 * }</pre>
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li><strong>Signature</strong>: exactly one parameter of type
 *       {@link io.flowwarden.stream.HistoryLostContext}; returns {@code void}
 *       ({@code IMPERATIVE}), {@code void} or {@code Mono<Void>}
 *       ({@code REACTIVE}). At most one {@code @Reconcile} method per
 *       {@link ChangeStream} class.</li>
 *   <li><strong>Barrier</strong>: the hook runs after the resume position has
 *       been resolved and <em>before</em> the cursor opens and before any
 *       checkpoint write. Nothing is delivered to the handlers meanwhile, so
 *       the reconciliation never interleaves with live events. Everything
 *       after {@link io.flowwarden.stream.HistoryLostContext#lostWindowEnd()
 *       lostWindowEnd()} is then delivered by the stream, at-least-once.</li>
 *   <li><strong>Durability</strong>: the expired tokens stay in the checkpoint
 *       until the hook has returned. A crash or a kill during the
 *       reconciliation leaves that marker in place and the next start calls
 *       the hook again — the hook must be <em>idempotent</em>, like the
 *       handlers.</li>
 *   <li><strong>Failure</strong>: a hook that throws stops the start with
 *       {@link io.flowwarden.stream.ReconciliationFailedException}, exactly
 *       like {@link io.flowwarden.stream.OnHistoryLost#FAIL FAIL}: the restart
 *       loop gives up, the lease is released under {@code SINGLE_LEADER}, the
 *       tokens are kept. A failed reconciliation never lets the stream resume
 *       on a stale view.</li>
 *   <li><strong>Oplog rolling during the hook</strong>: after the hook
 *       returns, the resolved position is validated again. If the
 *       reconciliation outlived the oplog window, a fresh position is taken
 *       and the hook is called once more
 *       ({@link io.flowwarden.stream.HistoryLostContext#attempt() attempt() == 2}),
 *       bounded to the small residual window; if that one rolls out as well,
 *       the start fails with a message saying the reconciliation does not fit
 *       in the oplog. Never a loop.</li>
 *   <li><strong>Thread</strong>: the hook runs on the thread that starts the
 *       stream (application startup, a managed restart attempt, a leadership
 *       acquisition), under the stream's lifecycle lock. An operator stop
 *       issued meanwhile waits for the hook to return. A reactive hook's
 *       {@code Mono<Void>} is awaited on that thread, never on a driver
 *       thread.</li>
 * </ul>
 *
 * <h2>Validation</h2>
 * <p>Rejected at bootstrap, on both the annotation and the
 * {@code StreamSpec} path: more than one {@code @Reconcile} method, a wrong
 * signature, and a hook that could never run — without {@link Checkpoint},
 * with {@code onHistoryLost = FAIL} (which refuses to start) or with
 * {@code startPosition = LATEST} (which never resumes).</p>
 *
 * <p>Sizing the oplog for the longest downtime the application expects
 * remains the operator's job; the idle heartbeat's probe warning is the
 * signal that it is too small.</p>
 *
 * @see io.flowwarden.stream.HistoryLostContext
 * @see io.flowwarden.stream.core.ReconcileHandler
 * @see Checkpoint#onHistoryLost()
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Reconcile {
}
