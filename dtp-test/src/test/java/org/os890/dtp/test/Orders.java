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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.ResultStatus;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.spi.Transactions;

/**
 * A small order flow of an application under test: the intake tracks orders, an order without customer goes back to
 * the intake, a valid one forks into a credit and a stock check and is shipped after both.
 */
class Orders {

    static final ItemType<Order, UUID> ITEM = Order.ITEM;

    static final String TABLE = "CREATE TABLE test_order (id UUID PRIMARY KEY, number VARCHAR(20) NOT NULL,"
            + " customer VARCHAR(20))";

    private Orders() {
    }

    /** The entity, with its item type (DQ-6). */
    public record Order(UUID id, String number, String customer) {

        public static final ItemType<Order, UUID> ITEM = ItemType.of("order", Order.class, UUID.class);
    }

    record IntakeParams(int count) {
    }

    enum IntakeOutcome implements ResultStatus {
        RECEIVED, NOTHING
    }

    enum IntakeSignal implements Signal {
        RECEIVED
    }

    enum CheckSignal implements Signal {
        VALID, NO_CUSTOMER
    }

    enum CheckResult implements Signal {
        OK
    }

    enum ShippingSignal implements Signal {
        SHIPPED
    }

    /** Receives orders O-1, O-2, ...; every third has no customer. */
    static class OrderIntake implements JobHandler<IntakeParams, IntakeOutcome> {

        private final Transactions tx;

        OrderIntake(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public JobResult<IntakeOutcome> execute(JobContext<IntakeParams> ctx) {
            int offset = tx.withConnection(c -> Sql.queryOne(c, "SELECT COUNT(*) FROM test_order",
                    rs -> rs.getInt(1)).orElse(0));
            for (int i = offset + 1; i <= offset + ctx.parameters().count(); i++) {
                UUID id = UUID.randomUUID();
                String number = "O-" + i;
                String customer = i % 3 == 0 ? null : "C-" + i;
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_order (id, number, customer) VALUES (?, ?, ?)",
                        id, number, customer));
                ctx.items().track(ItemRef.of(ITEM, id), number).signal(IntakeSignal.RECEIVED);
            }
            return JobResult.of(ctx.parameters().count() == 0 ? IntakeOutcome.NOTHING : IntakeOutcome.RECEIVED);
        }
    }

    /** Sends an order without customer back to the intake. */
    static class OrderCheck implements ItemProcessor<Order, UUID> {

        private final Transactions tx;

        OrderCheck(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public void process(TrackedItem<Order, UUID> item) {
            String customer = tx.withConnection(c -> Sql.queryOne(c, "SELECT customer FROM test_order WHERE id = ?",
                    rs -> rs.getString(1), item.ref().id())).orElse(null);
            if (customer == null) {
                item.signal(CheckSignal.NO_CUSTOMER, "no customer");
            } else {
                item.signal(CheckSignal.VALID);
            }
        }
    }

    /** A branch: the credit of the customer. */
    static class CreditCheck implements ItemProcessor<Order, UUID> {

        @Override
        public void process(TrackedItem<Order, UUID> item) {
            item.signal(CheckResult.OK);
        }
    }

    /** A branch: the stock. */
    static class StockCheck implements ItemProcessor<Order, UUID> {

        @Override
        public void process(TrackedItem<Order, UUID> item) {
            item.signal(CheckResult.OK);
        }
    }

    /** Ships an order; the carrier can be down for an order (a technical failure, retried). */
    static class Shipping implements ItemProcessor<Order, UUID> {

        final Set<String> carrierDownFor = ConcurrentHashMap.newKeySet();
        final Set<String> shipped = ConcurrentHashMap.newKeySet();
        final AtomicInteger shipments = new AtomicInteger();

        @Override
        public void process(TrackedItem<Order, UUID> item) {
            if (carrierDownFor.contains(item.businessKey())) {
                throw new IllegalStateException("carrier down");
            }
            shipped.add(item.businessKey());
            shipments.incrementAndGet();
            item.signal(ShippingSignal.SHIPPED);
        }
    }

    /** A report at 06:00 UTC. */
    static class DailyReport implements JobHandler<NoParams, IntakeOutcome> {

        final AtomicInteger runs = new AtomicInteger();

        @Override
        public JobResult<IntakeOutcome> execute(JobContext<NoParams> ctx) {
            runs.incrementAndGet();
            return JobResult.of(IntakeOutcome.RECEIVED);
        }
    }

    /** The flow. */
    static class OrderFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(OrderIntake.class)
                .on(IntakeSignal.RECEIVED).to(OrderCheck.class);
            flow.step(OrderCheck.class)
                .on(CheckSignal.VALID).fork(CreditCheck.class).and(StockCheck.class).joinAt(Shipping.class)
                .on(CheckSignal.NO_CUSTOMER).back(OrderIntake.class);
            flow.step(Shipping.class)
                .on(ShippingSignal.SHIPPED).complete();
        }
    }
}
