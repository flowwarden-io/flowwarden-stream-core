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
package io.flowwarden.stream.internal.discovery;

import io.flowwarden.stream.HistoryLostContext;
import io.flowwarden.stream.core.ReactiveReconcileHandler;
import io.flowwarden.stream.core.ReconcileHandler;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * Encapsulates a {@code @Reconcile}-annotated method — or its functional
 * equivalent for a contributed stream — and its invocation strategy.
 *
 * <p>Unlike {@link ErrorHandlerMethod}, which contains a failing user
 * callback, this invoker lets every failure out: a reconciliation that did
 * not complete must stop the start (the caller turns it into a
 * {@code ReconciliationFailedException}). A reactive return is awaited here,
 * on the calling thread — always a start thread, never a driver thread.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class ReconcileMethod {

    private final Method method;
    private final boolean reactiveReturn;
    private final ReconcileHandler handler;
    private final ReactiveReconcileHandler reactiveHandler;

    public ReconcileMethod(Method method, boolean reactiveReturn) {
        this.method = Objects.requireNonNull(method, "method must not be null");
        this.reactiveReturn = reactiveReturn;
        this.handler = null;
        this.reactiveHandler = null;
    }

    private ReconcileMethod(ReconcileHandler handler, ReactiveReconcileHandler reactiveHandler) {
        this.method = null;
        this.reactiveReturn = reactiveHandler != null;
        this.handler = handler;
        this.reactiveHandler = reactiveHandler;
    }

    /** Creates a {@link ReconcileMethod} backed by a functional handler (no reflection). */
    public static ReconcileMethod fromHandler(ReconcileHandler handler) {
        return new ReconcileMethod(Objects.requireNonNull(handler, "handler must not be null"), null);
    }

    /** Creates a {@link ReconcileMethod} backed by a reactive functional handler (no reflection). */
    public static ReconcileMethod fromReactiveHandler(ReactiveReconcileHandler handler) {
        return new ReconcileMethod(null, Objects.requireNonNull(handler, "handler must not be null"));
    }

    /** The reflective method, or {@code null} for a functional handler. */
    public Method method() {
        return method;
    }

    /** Whether the hook returns {@code Mono<Void>} (awaited on the calling thread). */
    public boolean isReactiveReturn() {
        return reactiveReturn;
    }

    /**
     * Runs the reconciliation and waits for it to complete.
     *
     * @param bean the target bean instance (ignored for a functional handler)
     * @param ctx  the lost-window bound
     * @throws Exception whatever the hook threw (unwrapped), or the error a
     *                   reactive hook signalled
     */
    public void invoke(Object bean, HistoryLostContext ctx) throws Exception {
        if (handler != null) {
            handler.reconcile(ctx);
            return;
        }
        if (reactiveHandler != null) {
            await(reactiveHandler.reconcile(ctx));
            return;
        }
        Object result;
        try {
            result = method.invoke(bean, ctx);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof Exception ex) {
                throw ex;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new IllegalStateException(cause);
        }
        if (reactiveReturn) {
            await((Mono<?>) result);
        }
    }

    private static void await(Mono<?> mono) {
        if (mono == null) {
            throw new IllegalStateException("@Reconcile hook returned a null Mono");
        }
        mono.block();
    }

    @Override
    public String toString() {
        if (method == null) {
            return reactiveReturn ? "functional(reconcileReactive)" : "functional(reconcile)";
        }
        return method.getDeclaringClass().getSimpleName() + "#" + method.getName();
    }
}
