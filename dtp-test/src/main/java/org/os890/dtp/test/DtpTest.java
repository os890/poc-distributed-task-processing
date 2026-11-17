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
package org.os890.dtp.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A test of process steps and flows on a cluster of simulated nodes (design 06 §6.4). Each test method that declares a
 * {@link DtpTestCluster} parameter gets a new cluster on its own database, an in-memory H2 database or a schema of its
 * own on PostgreSQL ({@link TestDatabase#fromEnvironment()}); the cluster is closed after the test (its nodes are
 * killed, the database or schema is dropped).
 *
 * <pre>{@code
 * @DtpTest
 * class OrderFlowTest {
 *
 *     @Test
 *     void validOrdersAreShipped(DtpTestCluster cluster) {
 *         cluster.definitions().define(OrderIntake.class).external().save();
 *         cluster.start(2, new OrderFlow(), new OrderIntake(), new OrderCheck(), new Shipping());
 *
 *         cluster.jobEngine().trigger(OrderIntake.class, new IntakeParams(3));
 *         cluster.awaitIdle();
 *
 *         assertThat(cluster.journey(Order.ITEM, "O-1"))
 *                 .hasPath(OrderIntake.class, OrderCheck.class, Shipping.class)
 *                 .isCompleted();
 *     }
 * }
 * }</pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ExtendWith(DtpTestExtension.class)
public @interface DtpTest {
}
