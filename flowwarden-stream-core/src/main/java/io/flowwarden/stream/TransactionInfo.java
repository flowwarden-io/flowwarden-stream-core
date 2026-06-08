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
package io.flowwarden.stream;

import org.bson.BsonDocument;

/**
 * MongoDB transaction metadata grouped atomically.
 *
 * <p>MongoDB emits {@code lsid} (logical session id) and {@code txnNumber}
 * together on every event that belongs to a transaction, or omits both for
 * standalone operations. Grouping them in a record prevents the
 * "one present without the other" invalid state by construction; absence
 * is represented by an empty {@link java.util.Optional} at the
 * {@link ChangeStreamContext#getTransactionInfo()} call site.</p>
 *
 * <p>Useful for handlers that need to group events of the same transaction
 * — audit logs, aggregation, atomicity-preserving downstream propagation.</p>
 *
 * @param lsid      MongoDB logical session id
 * @param txnNumber MongoDB transaction number
 */
public record TransactionInfo(BsonDocument lsid, long txnNumber) {
}
