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
package org.os890.dtp.cdi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.os890.dtp.core.DtpException;

/** Transaction demarcation of {@link JtaTransactions}. */
class JtaTransactionsTest {

    private final RecordingTransactionManager tm = new RecordingTransactionManager();
    private final JtaTransactions tx = new JtaTransactions(tm, dataSource());

    @Test
    void requiredJoinsAndRequiresNewSuspends() {
        String result = tx.required(() -> tx.required(() -> tx.requiresNew(() -> "done")));
        assertThat(result).isEqualTo("done");
        assertThat(tm.log()).containsExactly("begin 1", "suspend 1", "begin 2", "commit 2", "resume 1", "commit 1");
    }

    @Test
    void failuresRollBackAndCheckedExceptionsAreWrapped() {
        assertThatThrownBy(() -> tx.required(() -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> tx.required(() -> {
            throw new java.io.IOException("io");
        })).isInstanceOf(DtpException.class).hasCauseInstanceOf(java.io.IOException.class);
        assertThat(tm.log()).containsExactly("begin 1", "rollback 1", "begin 2", "rollback 2");
    }

    @Test
    void actionsAfterTheCommitRunOnlyAfterASuccessfulCommit() {
        tx.afterCommit(() -> tm.log().add("without transaction"));
        tx.required(() -> {
            tx.afterCommit(() -> tm.log().add("after commit"));
            return tx.requiresNew(() -> {
                tx.afterCommit(() -> tm.log().add("after inner commit"));
                return null;
            });
        });
        assertThatThrownBy(() -> tx.required(() -> {
            tx.afterCommit(() -> tm.log().add("never"));
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);
        tx.required(() -> {
            tx.afterCommit(() -> {
                throw new IllegalStateException("a failing observer is logged");
            });
            return null;
        });
        assertThat(tm.log()).containsExactly("without transaction", "begin 1", "suspend 1", "begin 2", "commit 2",
                "after inner commit", "resume 1", "commit 1", "after commit", "begin 3", "rollback 3", "begin 4",
                "commit 4");
    }

    @Test
    void actionsInATransactionOfTheApplicationRunAfterItsCompletion() throws Exception {
        tm.begin();
        tx.required(() -> {
            tx.afterCommit(() -> tm.log().add("after the commit of the application"));
            return null;
        });
        tm.commit();
        tm.begin();
        tx.afterCommit(() -> tm.log().add("never"));
        tm.rollback();
        assertThat(tm.log()).containsExactly("begin 1", "commit 1", "after the commit of the application", "begin 2",
                "rollback 2");
    }

    @Test
    void connectionsComeFromTheDataSource() {
        String product = tx.withConnection(c -> c.getMetaData().getDatabaseProductName());
        assertThat(product).isEqualTo("H2");
        assertThatThrownBy(() -> tx.withConnection(c -> c.createStatement().executeQuery("SELECT * FROM missing")))
                .isInstanceOf(DtpException.class);
    }

    private static JdbcDataSource dataSource() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:jta-test;DB_CLOSE_DELAY=-1");
        return h2;
    }
}
