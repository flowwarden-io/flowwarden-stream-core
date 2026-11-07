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
package io.flowwarden.stream.internal.checkpoint;

import com.mongodb.MongoException;

import java.util.Set;

/**
 * Classifies a throwable as a <em>non-resumable position</em> error: the
 * server refuses to open a change stream from the given resume token or
 * operation time because the oplog no longer holds it. Such a position never
 * becomes valid again — an oplog window that grows back only adds newer
 * entries — so retrying it is pointless, whatever the window does next.
 *
 * <p>Recognized by server code — 286 {@code ChangeStreamHistoryLost}, 136
 * {@code CappedPositionLost}, 280 {@code ChangeStreamFatalError} — or by the
 * {@code NonResumableChangeStreamError} label the server attaches to them.
 * The driver, Spring Data's exception translation and Reactor each wrap the
 * original {@link MongoException}, so the whole cause chain is inspected.</p>
 *
 * <p>This class is internal and not part of the public API.</p>
 */
public final class HistoryLostErrors {

    private static final Set<Integer> NON_RESUMABLE_CODES = Set.of(286, 136, 280);
    private static final String NON_RESUMABLE_LABEL = "NonResumableChangeStreamError";
    private static final int MAX_CAUSE_DEPTH = 32;

    private HistoryLostErrors() {
    }

    /**
     * A stable identity for a cursor death — the innermost driver exception's
     * class and server code when there is one, the throwable's class
     * otherwise — so a restart loop can tell "the same failure again" from a
     * new one without comparing messages.
     */
    public static String signatureOf(Throwable throwable) {
        Throwable cause = throwable;
        for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (cause instanceof MongoException me) {
                return me.getClass().getName() + ":" + me.getCode();
            }
            cause = cause.getCause();
        }
        return throwable.getClass().getName();
    }

    /**
     * Whether the throwable, or any cause beneath it, is a non-resumable
     * change stream position error.
     */
    public static boolean isHistoryLost(Throwable throwable) {
        Throwable cause = throwable;
        for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (cause instanceof MongoException me
                    && (NON_RESUMABLE_CODES.contains(me.getCode())
                            || me.hasErrorLabel(NON_RESUMABLE_LABEL))) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
