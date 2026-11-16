/*
 * Copyright 2026 os890
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.os890.dtp.core.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;

import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.SubjectType;
import org.os890.dtp.core.jdbc.PackageStore;
import org.os890.dtp.core.jdbc.PackageStore.PackageRow;

/**
 * A failed attempt of a work package with the retry policy of its job: pending again after the backoff, or waiting
 * for a decision once the retries are exhausted (technical failures go to operations, DD-29). Used by the run that
 * processes the package and by rejections that arrive later (slice 14).
 */
class PackageFailures {

    private final Decisions decisions;
    private final PackageStore store;

    PackageFailures(Decisions decisions, PackageStore store) {
        this.decisions = decisions;
        this.store = store;
    }

    /**
     * A failed attempt of a package.
     *
     * @return {@code true} if its retries are exhausted: it waits for a decision
     */
    boolean failed(Connection c, PackageRow row, JobDefinition job, UUID runId, FailureKind kind, String reason,
                   Instant now) throws SQLException {
        int attempts = row.attempts() + 1;
        if (attempts <= job.retryMax()) {
            store.failed(c, row.id(), runId, attempts, now.plus(job.retryBackoff().multipliedBy(attempts)), reason,
                    now);
            return false;
        }
        store.failed(c, row.id(), runId, attempts, null, reason, now);
        String why = "work package " + row.seq() + " (" + row.groupKey() + "): retries exhausted (" + job.retryMax()
                + ")" + (reason == null ? "" : ": " + reason);
        decisions.request(c, new Decisions.NewRequest(DecisionKind.CHOICE, SubjectType.WORK_PACKAGE, row.id(),
                job.id(), runId, why, Decisions.builtIn(BuiltInOption.PACKAGE), null, null,
                kind == FailureKind.TECHNICAL ? Decisions.OPERATORS : null), now);
        return true;
    }
}
