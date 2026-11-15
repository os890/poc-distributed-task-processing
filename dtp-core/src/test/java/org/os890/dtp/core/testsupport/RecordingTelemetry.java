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
package org.os890.dtp.core.testsupport;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.os890.dtp.core.spi.ClusterState;
import org.os890.dtp.core.spi.Telemetry;

/** Records what the engine of one node reports, with fake spans that have W3C trace ids. */
public class RecordingTelemetry implements Telemetry {

    /** An item result. */
    public record Item(String step, String lane, String result, String failureKind) {
    }

    /** A wait in an inbox. */
    public record Wait(String step, String lane, Duration duration) {
    }

    /** A step and a value, e.g. a rollback reason or a run outcome. */
    public record Entry(String step, String value) {
    }

    /** A decision. */
    public record Decision(String step, String kind, String group) {
    }

    /** A finished span. */
    public record SpanRecord(String step, UUID runId, int attempt, String traceId, List<String> links, int items,
                             Throwable failure) {
    }

    public final List<Item> items = new CopyOnWriteArrayList<>();
    public final List<Wait> waits = new CopyOnWriteArrayList<>();
    public final List<Entry> technical = new CopyOnWriteArrayList<>();
    public final List<String> units = new CopyOnWriteArrayList<>();
    public final List<Entry> rollbacks = new CopyOnWriteArrayList<>();
    public final List<Entry> runs = new CopyOnWriteArrayList<>();
    public final List<Duration> claims = new CopyOnWriteArrayList<>();
    public final AtomicInteger conflicts = new AtomicInteger();
    public final List<String> renewals = new CopyOnWriteArrayList<>();
    public final List<String> lost = new CopyOnWriteArrayList<>();
    public final List<String> takeovers = new CopyOnWriteArrayList<>();
    public final List<String> elections = new CopyOnWriteArrayList<>();
    public final List<String> acknowledged = new CopyOnWriteArrayList<>();
    /** Rejected packages as {@code step:reason}. */
    public final List<String> rejected = new CopyOnWriteArrayList<>();
    public final List<Decision> created = new CopyOnWriteArrayList<>();
    public final List<Decision> decided = new CopyOnWriteArrayList<>();
    public final List<SpanRecord> spans = new CopyOnWriteArrayList<>();
    /** Every report of the cluster state, empty for a revocation. */
    public final List<Optional<ClusterState>> clusterReports = new CopyOnWriteArrayList<>();

    private final ThreadLocal<Deque<String[]>> current = ThreadLocal.withInitial(ArrayDeque::new);

    /** A new W3C traceparent, e.g. of an incoming request. */
    public static String newTraceParent() {
        return "00-" + hex(16) + "-" + hex(8) + "-01";
    }

    /**
     * Runs work as if inside a traced request.
     *
     * @param traceParent the traceparent of the request
     * @param work        the work
     * @param <T>         the result type
     * @return the result
     */
    public <T> T inRequest(String traceParent, Supplier<T> work) {
        String[] parts = traceParent.split("-");
        current.get().push(new String[] {parts[1], parts[2]});
        try {
            return work.get();
        } finally {
            current.get().pop();
        }
    }

    /** The latest cluster state, empty if revoked or never reported. */
    public Optional<ClusterState> cluster() {
        return clusterReports.isEmpty() ? Optional.empty() : clusterReports.getLast();
    }

    @Override
    public boolean tracing() {
        return true;
    }

    @Override
    public void itemProcessed(String step, String lane, String result, String failureKind) {
        items.add(new Item(step, lane, result, failureKind));
    }

    @Override
    public void itemWaited(String step, String lane, Duration wait) {
        waits.add(new Wait(step, lane, wait));
    }

    @Override
    public void technicalFailure(String step, String errorType) {
        technical.add(new Entry(step, errorType));
    }

    @Override
    public void unitOfWork(String step, Duration duration) {
        units.add(step);
    }

    @Override
    public void rollback(String step, String reason) {
        rollbacks.add(new Entry(step, reason));
    }

    @Override
    public void runFinished(String step, String outcome, Duration duration) {
        runs.add(new Entry(step, outcome));
    }

    @Override
    public void claimed(Duration lag) {
        claims.add(lag);
    }

    @Override
    public void claimConflict() {
        conflicts.incrementAndGet();
    }

    @Override
    public void leaseRenewed(String step) {
        renewals.add(step);
    }

    @Override
    public void leaseLost(String step) {
        lost.add(step);
    }

    @Override
    public void leaseTakenOver(String step) {
        takeovers.add(step);
    }

    @Override
    public void leaderElected(String role) {
        elections.add(role);
    }

    @Override
    public void packageAcknowledged(String step, Duration sinceSent) {
        acknowledged.add(step);
    }

    @Override
    public void packageRejected(String step, String reason) {
        rejected.add(step + ":" + reason);
    }

    @Override
    public void decisionCreated(String step, String kind, String group) {
        created.add(new Decision(step, kind, group));
    }

    @Override
    public void decisionDecided(String step, String kind, String group) {
        decided.add(new Decision(step, kind, group));
    }

    @Override
    public void clusterState(ClusterState state) {
        clusterReports.add(Optional.ofNullable(state));
    }

    @Override
    public Span startUnitOfWork(String step, UUID runId, int attempt, List<String> links) {
        String traceId = hex(16);
        current.get().push(new String[] {traceId, hex(8)});
        int[] count = {0};
        Throwable[] failure = {null};
        return new Span() {
            @Override
            public void items(int items) {
                count[0] = items;
            }

            @Override
            public void failed(Throwable error) {
                failure[0] = error;
            }

            @Override
            public void close() {
                current.get().pop();
                spans.add(new SpanRecord(step, runId, attempt, traceId, List.copyOf(links), count[0], failure[0]));
            }
        };
    }

    @Override
    public String currentTraceParent() {
        String[] span = current.get().peek();
        return span == null ? null : "00-" + span[0] + "-" + span[1] + "-01";
    }

    @Override
    public String currentTraceId() {
        String[] span = current.get().peek();
        return span == null ? null : span[0];
    }

    private static String hex(int bytes) {
        byte[] random = new byte[bytes];
        ThreadLocalRandom.current().nextBytes(random);
        return HexFormat.of().formatHex(random);
    }
}
