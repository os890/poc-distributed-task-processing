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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.os890.dtp.api.ApprovalResult;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.DecisionRequest;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.event.DecisionFallbackDue;
import org.os890.dtp.api.event.DecisionMade;
import org.os890.dtp.api.event.DecisionRequested;
import org.os890.dtp.api.event.VoteCast;
import org.os890.dtp.api.event.WindowClosed;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.BranchState;
import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.domain.ItemState;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.OccurrenceState;
import org.os890.dtp.core.domain.SubjectType;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.jdbc.DecisionStore;
import org.os890.dtp.core.jdbc.DecisionStore.RequestRow;
import org.os890.dtp.core.jdbc.DecisionStore.VoteRow;
import org.os890.dtp.core.jdbc.FlowInstanceStore;
import org.os890.dtp.core.jdbc.ItemStore.ItemEvent;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;
import org.os890.dtp.core.jdbc.PackageStore;
import org.os890.dtp.core.jdbc.PackageStore.PackageRow;
import org.os890.dtp.core.jdbc.WindowStore;
import org.os890.dtp.core.spi.Transactions;

/**
 * Decision requests (DD-5): one mechanism for everything that waits for a person. A user picks an option
 * ({@code CHOICE}) or a quorum of distinct users approves ({@code APPROVAL}, DD-16). Requests assigned to a team can
 * only be seen and decided by its members (DD-24). A decided item goes back into the inbox of the step that requested
 * the decision, with the decision attached (DD-19). Requests never expire (D38), but the requesting job can name a
 * fallback job: one run of it per request that stays open longer than the fallback delay, e.g. to remind people.
 * Requests, votes, decisions and planned fallbacks are published as domain events after the commit.
 */
public class Decisions {

    /** Group of the operators, responsible for technical failures and abandoned runs (DD-29). */
    public static final String OPERATORS = "dtp-operator";

    private static final int FALLBACKS_PER_TICK = 100;

    private final EngineContext ctx;
    private final DecisionStore store = new DecisionStore();

    public Decisions(EngineContext ctx) {
        this.ctx = ctx;
    }

    /** Votes of an approval. */
    public enum Vote {
        APPROVE, REJECT
    }

    /**
     * The user who decides or votes. The caller authenticates the user (e.g. MP JWT) and checks the role.
     *
     * @param user   the user name
     * @param groups the groups of the user
     */
    public record Decider(String user, Set<String> groups) {

        public Decider {
            Objects.requireNonNull(user, "user");
            groups = Set.copyOf(groups);
        }

        /**
         * A decider.
         *
         * @param user   the user name
         * @param groups the groups
         * @return the decider
         */
        public static Decider of(String user, String... groups) {
            return new Decider(user, Set.of(groups));
        }

        boolean isMemberOf(String group) {
            return group == null || groups.contains(group);
        }
    }

    /**
     * An option of a request.
     *
     * @param name    the name
     * @param label   the label
     * @param builtIn {@code true} if the framework applies it, {@code false} for an option of the requesting step
     */
    public record OptionView(String name, String label, boolean builtIn) {
    }

    /**
     * A vote of an approval.
     *
     * @param voter   the user
     * @param vote    APPROVE or REJECT
     * @param comment the comment
     * @param votedAt when
     */
    public record VoteView(String voter, String vote, String comment, Instant votedAt) {
    }

    /**
     * A decision request.
     *
     * @param id                the id
     * @param kind              CHOICE or APPROVAL
     * @param subjectType       ITEM or RUN
     * @param subjectId         the item or run
     * @param itemType          the item type (items only)
     * @param businessKey       the business key (items only)
     * @param job               the job that requested the decision
     * @param reason            why
     * @param options           the options
     * @param requiredApprovals the quorum (approvals only)
     * @param responsibleGroup  the responsible team, or {@code null}
     * @param state             OPEN, DECIDED or WITHDRAWN (its run was cancelled)
     * @param votes             the votes (approvals only)
     * @param decidedOption     the decision
     * @param decidedBy         who decided
     * @param decidedAt         when
     * @param comment           the comment of the decision
     * @param createdAt         when it was requested
     */
    public record DecisionView(UUID id, String kind, String subjectType, UUID subjectId, String itemType,
                               String businessKey, String job, String reason, List<OptionView> options,
                               Integer requiredApprovals, String responsibleGroup, String state, List<VoteView> votes,
                               String decidedOption, String decidedBy, Instant decidedAt, String comment,
                               Instant createdAt) {
    }

    /**
     * A new request.
     *
     * @param kind              CHOICE or APPROVAL
     * @param subjectType       ITEM or RUN
     * @param subjectId         the item or run
     * @param requestedByJobId  the job the decided item goes back to
     * @param runId             the requesting run, or {@code null}
     * @param reason            why
     * @param options           the options
     * @param optionType        the enum of custom options, or {@code null}
     * @param requiredApprovals the quorum, or {@code null}
     * @param responsibleGroup  the team, or {@code null}
     */
    record NewRequest(DecisionKind kind, SubjectType subjectType, UUID subjectId, UUID requestedByJobId, UUID runId,
                      String reason, List<OptionView> options, Class<?> optionType, Integer requiredApprovals,
                      String responsibleGroup) {
    }

    /**
     * The responsible teams of the open requests (e.g. to check the teams of a caller one by one).
     *
     * @return the teams
     */
    public Set<String> openTeams() {
        return ctx.tx().withConnection(c -> store.open(c, ctx.namespace()).stream().map(RequestRow::responsibleGroup)
                .filter(Objects::nonNull).collect(Collectors.toCollection(TreeSet::new)));
    }

    /**
     * The open requests the user may decide, oldest first.
     *
     * @param who the user
     * @return the requests
     */
    public List<DecisionView> open(Decider who) {
        return ctx.tx().withConnection(c -> {
            List<DecisionView> views = new ArrayList<>();
            for (RequestRow row : store.open(c, ctx.namespace())) {
                if (who.isMemberOf(row.responsibleGroup())) {
                    views.add(view(c, row));
                }
            }
            return views;
        });
    }

    /**
     * A request.
     *
     * @param id the request
     * @return the request
     */
    public DecisionView get(UUID id) {
        return ctx.tx().withConnection(c -> view(c, request(c, id, false)));
    }

    /**
     * The requests of an item or run, newest first.
     *
     * @param subjectId the item or run
     * @return the requests
     */
    public List<DecisionView> ofSubject(UUID subjectId) {
        return ctx.tx().withConnection(c -> {
            List<DecisionView> views = new ArrayList<>();
            for (RequestRow row : store.ofSubject(c, subjectId)) {
                views.add(view(c, row));
            }
            return views;
        });
    }

    /**
     * Decides a {@code CHOICE} request and applies the decision in the same transaction.
     *
     * @param id      the request
     * @param option  the chosen option
     * @param who     the user
     * @param comment the comment, or {@code null}
     */
    public void decide(UUID id, String option, Decider who, String comment) {
        inTx(c -> {
            RequestRow request = request(c, id, true);
            check(request, who, DecisionKind.CHOICE);
            if (optionViews(request).stream().noneMatch(o -> o.name().equals(option))) {
                throw new DtpException("decision request " + id + " has no option " + option);
            }
            decideAndApply(c, request, option, who.user(), comment);
            return null;
        });
    }

    /**
     * Votes on an {@code APPROVAL} request. One vote per user; one rejection rejects, the quorum of distinct approvals
     * approves (DD-16).
     *
     * @param id      the request
     * @param vote    the vote
     * @param who     the user
     * @param comment the comment, or {@code null}
     * @return {@code true} if this vote decided the request
     */
    public boolean vote(UUID id, Vote vote, Decider who, String comment) {
        return inTx(c -> {
            RequestRow request = request(c, id, true);
            check(request, who, DecisionKind.APPROVAL);
            List<VoteRow> votes = store.votes(c, id);
            if (votes.stream().anyMatch(v -> v.voter().equals(who.user()))) {
                throw new DtpException(who.user() + " voted already on decision request " + id);
            }
            Instant now = ctx.clock().now(c);
            store.insertVote(c, id, new VoteRow(who.user(), vote.name(), comment, now));
            itemEvent(c, request, ItemEventType.VOTED, null, vote + " by " + who.user() + suffix(comment), now);
            ctx.publishAfterCommit(new VoteCast(event(c, request), who.user(), vote.name(), comment));
            long approvals = votes.stream().filter(v -> Vote.APPROVE.name().equals(v.vote())).count()
                    + (vote == Vote.APPROVE ? 1 : 0);
            if (vote == Vote.REJECT) {
                decideAndApply(c, request, BuiltInOption.REJECTED.name(), who.user(), comment);
                return true;
            }
            if (approvals >= request.requiredApprovals()) {
                decideAndApply(c, request, BuiltInOption.APPROVED.name(), who.user(), comment);
                return true;
            }
            return false;
        });
    }

    /**
     * Inserts a request in the caller's transaction.
     *
     * @param c   the connection
     * @param r   the request
     * @param now the DB time
     * @return the request id
     * @throws SQLException on DB errors
     */
    UUID request(Connection c, NewRequest r, Instant now) throws SQLException {
        UUID id = UUID.randomUUID();
        JobDefinition job = ctx.catalog().byId(r.requestedByJobId());
        RequestRow row = new RequestRow(id, ctx.namespace(), r.kind(), r.subjectType(), r.subjectId(),
                r.requestedByJobId(), r.runId(), r.reason(), ctx.json().write(r.options()),
                r.optionType() == null ? null : r.optionType().getName(), r.requiredApprovals(),
                r.responsibleGroup(), "OPEN", null, null, null, null, now, job.fallbackJobId(),
                job.hasDecisionFallback() ? now.plus(job.fallbackDelay()) : null, null);
        store.insert(c, row);
        ctx.publishAfterCommit(new DecisionRequested(event(c, row)));
        String step = job.name();
        ctx.tx().afterCommit(() -> ctx.telemetry().decisionCreated(step, row.kind().name(), row.responsibleGroup()));
        return id;
    }

    /**
     * Plans the due fallback runs (housekeeping leader): one run of the fallback job per open request, with the
     * request as typed parameters ({@link DecisionRequest}). The request stays open (D38). Each request in its own
     * transaction.
     *
     * @param planner the run planner
     * @return the number of planned runs
     */
    int planFallbacks(RunPlanner planner) {
        List<UUID> due = ctx.tx().withConnection(c -> store.dueFallbacks(c, ctx.namespace(), ctx.clock().now(c),
                FALLBACKS_PER_TICK));
        int planned = 0;
        for (UUID id : due) {
            if (inTx(c -> planFallback(c, planner, id))) {
                planned++;
            }
        }
        return planned;
    }

    private boolean planFallback(Connection c, RunPlanner planner, UUID id) throws SQLException {
        RequestRow request = request(c, id, true);
        if (!request.isOpen() || request.fallbackRunId() != null) {
            return false;
        }
        JobDefinition job = ctx.catalog().byId(request.fallbackJobId());
        DecisionRequest view = event(c, request);
        Instant now = ctx.clock().now(c);
        RunPlanner.Planned run = planner.plan(c, job, new RunPlanner.Trigger(TriggerType.FALLBACK, "fallback:" + id,
                "decision request " + id + " open since " + request.createdAt(), ctx.json().write(view), null), now);
        if (run.runId() == null || !store.fallbackPlanned(c, id, run.runId())) {
            return false;
        }
        ctx.publishAfterCommit(new DecisionFallbackDue(view, job.name(), run.runId()));
        return true;
    }

    /**
     * Options of a custom option enum, plus {@code DISCARD} unless the enum has its own.
     *
     * @param type the enum
     * @return the options
     */
    static List<OptionView> optionsOf(Class<? extends DecisionOption> type) {
        DecisionOption[] constants = type.getEnumConstants();
        if (constants == null || constants.length == 0) {
            throw new IllegalArgumentException(type.getName() + " has to be an enum with at least one constant");
        }
        List<OptionView> options = new ArrayList<>();
        Arrays.stream(constants).forEach(o -> options.add(new OptionView(o.name(), o.label(), false)));
        if (options.stream().noneMatch(o -> o.name().equals(BuiltInOption.DISCARD.name()))) {
            options.add(builtIn(BuiltInOption.DISCARD));
        }
        return options;
    }

    /**
     * Options of a custom option enum for a run ({@code JobContext#requestDecision}), plus {@code FAIL} unless the
     * enum has its own.
     *
     * @param type the enum
     * @return the options
     */
    static List<OptionView> optionsOfRun(Class<? extends DecisionOption> type) {
        List<OptionView> options = new ArrayList<>(optionsOf(type).stream()
                .filter(o -> !(o.builtIn() && BuiltInOption.DISCARD.name().equals(o.name()))).toList());
        if (options.stream().noneMatch(o -> o.name().equals(BuiltInOption.FAIL.name()))) {
            options.add(builtIn(BuiltInOption.FAIL));
        }
        return options;
    }

    /**
     * Built-in options.
     *
     * @param options the options
     * @return the views
     */
    static List<OptionView> builtIn(List<BuiltInOption> options) {
        return options.stream().map(Decisions::builtIn).toList();
    }

    /**
     * The custom option of the decision that brought an item back.
     *
     * @param requestId the request, or {@code null}
     * @param type      the option enum
     * @param <O>       the option type
     * @return the option, empty if the request had another option type
     */
    <O extends Enum<O> & DecisionOption> Optional<O> decisionOf(UUID requestId, Class<O> type) {
        if (requestId == null) {
            return Optional.empty();
        }
        RequestRow request = ctx.tx().withConnection(c -> request(c, requestId, false));
        if (!type.getName().equals(request.optionType()) || request.decidedOption() == null) {
            return Optional.empty();
        }
        return Arrays.stream(type.getEnumConstants()).filter(o -> o.name().equals(request.decidedOption()))
                     .findFirst();
    }

    /**
     * The custom option of the latest decided request about a run.
     *
     * @param runId the run
     * @param type  the option enum
     * @param <O>   the option type
     * @return the option, empty if no request of this type was decided
     */
    <O extends Enum<O> & DecisionOption> Optional<O> decisionOfRun(UUID runId, Class<O> type) {
        return decisionOf(latestDecided(runId, r -> type.getName().equals(r.optionType())), type);
    }

    /**
     * The latest approval about a run.
     *
     * @param runId the run
     * @return the result, empty if no approval was decided
     */
    Optional<ApprovalResult> approvalOfRun(UUID runId) {
        return approvalOf(latestDecided(runId, r -> r.kind() == DecisionKind.APPROVAL));
    }

    private UUID latestDecided(UUID runId, Predicate<RequestRow> filter) {
        return ctx.tx().withConnection(c -> store.ofSubject(c, runId)).stream()
                  .filter(r -> r.subjectType() == SubjectType.RUN && r.decidedAt() != null && filter.test(r))
                  .max(Comparator.comparing(RequestRow::decidedAt)).map(RequestRow::id).orElse(null);
    }

    /**
     * The approval that brought an item back.
     *
     * @param requestId the request, or {@code null}
     * @return the result, empty if it wasn't an approval
     */
    Optional<ApprovalResult> approvalOf(UUID requestId) {
        if (requestId == null) {
            return Optional.empty();
        }
        return ctx.tx().withConnection(c -> {
            RequestRow request = request(c, requestId, false);
            if (request.kind() != DecisionKind.APPROVAL || request.decidedOption() == null) {
                return Optional.empty();
            }
            List<String> approvers = store.votes(c, requestId).stream()
                    .filter(v -> Vote.APPROVE.name().equals(v.vote())).map(VoteRow::voter).toList();
            return Optional.of(new ApprovalResult(BuiltInOption.APPROVED.name().equals(request.decidedOption()),
                    approvers, request.decidedBy(), request.comment()));
        });
    }

    private void decideAndApply(Connection c, RequestRow request, String option, String user, String comment)
            throws SQLException {
        Instant now = ctx.clock().now(c);
        if (!store.decide(c, request.id(), option, user, comment, now)) {
            throw new DtpException("decision request " + request.id() + " was decided already");
        }
        String reason = option + " by " + user + suffix(comment);
        ctx.publishAfterCommit(new DecisionMade(event(c, request), option, user, comment));
        String step = ctx.catalog().byId(request.requestedByJobId()).name();
        ctx.tx().afterCommit(() -> ctx.telemetry().decisionDecided(step, request.kind().name(),
                request.responsibleGroup()));
        if (request.subjectType() == SubjectType.RUN) {
            // an abandoned run (RETRY, FAIL, SKIP) or a run that asked itself (its options, FAIL, an approval):
            // FAIL and SKIP finish it, everything else plans it again, the handler reads the decision
            boolean builtIn = optionViews(request).stream().anyMatch(o -> o.name().equals(option) && o.builtIn());
            RunOutcome outcome = !builtIn ? null : switch (BuiltInOption.valueOf(option)) {
                case FAIL -> RunOutcome.FAILED;
                case SKIP -> RunOutcome.SKIPPED;
                default -> null;
            };
            if (!ctx.runs().decided(c, request.subjectId(), outcome, now)) {
                throw new DtpException("run " + request.subjectId() + " doesn't wait for a decision");
            }
            new FlowInstances(ctx).runEnded(c, request.subjectId(), now);
            return;
        }
        if (request.subjectType() == SubjectType.WORK_PACKAGE) {
            decidePackage(c, request, BuiltInOption.DISCARD.name().equals(option), reason, now);
            return;
        }
        if (request.subjectType() == SubjectType.WINDOW_OCCURRENCE) {
            decideOccurrence(c, request, BuiltInOption.valueOf(option), now);
            return;
        }
        ItemRow item = ctx.items().load(c, request.subjectId());
        boolean branch = item.state() == ItemState.FORKED;
        if (branch ? !waitingBranch(c, request) : item.state() != ItemState.WAITING_FOR_DECISION) {
            throw new DtpException("item " + item.businessKey() + " doesn't wait for a decision");
        }
        itemEvent(c, request, ItemEventType.DECIDED, request.requestedByJobId(), reason, now);
        boolean builtIn = optionViews(request).stream().anyMatch(o -> o.name().equals(option) && o.builtIn());
        if (builtIn && BuiltInOption.DISCARD.name().equals(option)) {
            // the branches first, then the item: the order in which a finishing branch locks them
            ctx.items().cancelBranches(c, item.id(), now);
            ctx.items().lock(c, item.id());
            ctx.items().setState(c, item.id(), ItemState.DISCARDED, item.attempts(), now);
            itemEvent(c, request, ItemEventType.DISCARDED, null, reason, now);
            return;
        }
        if (branch) {
            // a decision about a branch (DD-22): the branch goes back into the inbox of its step
            ctx.items().requeueBranchAfterDecision(c, item.id(), request.requestedByJobId(), request.id(), reason,
                    now);
            return;
        }
        if (builtIn && BuiltInOption.RESET_VISITS.name().equals(option)) {
            ctx.items().resetVisits(c, item.id(), request.requestedByJobId());
        }
        ctx.items().requeueAfterDecision(c, item.id(), request.requestedByJobId(), request.id(), reason, now);
    }

    /**
     * A decision about a work package: pending again with its attempts reset (the next run of the job processes
     * it, within its processing slot, D39), or discarded with its items.
     */
    private void decidePackage(Connection c, RequestRow request, boolean discard, String reason, Instant now)
            throws SQLException {
        if (!new PackageStore(ctx.dialect()).decided(c, request.subjectId(), discard, now)) {
            throw new DtpException("work package " + request.subjectId() + " doesn't wait for a decision");
        }
        if (discard) {
            for (ItemRow item : ctx.items().lockPackaged(c, request.subjectId())) {
                ctx.items().setState(c, item.id(), ItemState.DISCARDED, item.attempts(), now);
                ctx.items().append(c, new ItemEvent(item.id(), ctx.namespace(), ItemEventType.DISCARDED,
                        request.requestedByJobId(), null, null, null, ctx.nodeId(), reason, null, null, null,
                        request.id(), request.subjectId()), now);
            }
        }
    }

    /**
     * Applies a decision on a window occurrence; the window coordinator acts on the new state in its next cycle.
     */
    private void decideOccurrence(Connection c, RequestRow request, BuiltInOption option, Instant now)
            throws SQLException {
        WindowStore windows = new WindowStore();
        boolean applied = switch (option) {
            case KEEP_WAITING -> windows.decided(c, request.subjectId(), OccurrenceState.QUIESCING, false, false, now);
            case FORCE_OPEN -> windows.decided(c, request.subjectId(), OccurrenceState.QUIESCING, true, false, now);
            case RETRY -> windows.decided(c, request.subjectId(), OccurrenceState.OPEN, false, true, now);
            case SKIP_OCCURRENCE -> windows.decided(c, request.subjectId(), OccurrenceState.SKIPPED, false, false,
                    now);
            default -> throw new DtpException("option " + option + " doesn't apply to a window occurrence");
        };
        if (!applied) {
            throw new DtpException("window occurrence " + request.subjectId() + " doesn't wait for a decision");
        }
        if (option == BuiltInOption.SKIP_OCCURRENCE) {
            ctx.publishAfterCommit(new WindowClosed(WindowCoordinator.occurrence(windows.load(c, request.subjectId())
                    .orElseThrow()), true, null));
        }
    }

    private boolean waitingBranch(Connection c, RequestRow request) throws SQLException {
        return ctx.items().branch(c, request.subjectId(), request.requestedByJobId())
                  .filter(b -> b.state() == BranchState.WAITING_FOR_DECISION).isPresent();
    }

    private void itemEvent(Connection c, RequestRow request, ItemEventType type, UUID toJob, String reason,
                           Instant now) throws SQLException {
        if (request.subjectType() == SubjectType.ITEM) {
            ctx.items().append(c, new ItemEvent(request.subjectId(), ctx.namespace(), type,
                    request.requestedByJobId(), toJob, null, null, ctx.nodeId(), reason, null, null, null,
                    request.id(), null), now);
        }
    }

    private static void check(RequestRow request, Decider who, DecisionKind kind) {
        if (request.kind() != kind) {
            throw new DtpException("decision request " + request.id() + " is an " + request.kind()
                    + (kind == DecisionKind.CHOICE ? ", vote instead" : ", decide instead"));
        }
        if (!request.isOpen()) {
            throw new DtpException("decision request " + request.id() + " was "
                    + ("WITHDRAWN".equals(request.state()) ? "withdrawn: " + request.comment() : "decided already"));
        }
        if (!who.isMemberOf(request.responsibleGroup())) {
            throw new DtpException("decision request " + request.id() + " is assigned to "
                    + request.responsibleGroup() + ", " + who.user() + " isn't a member");
        }
    }

    private RequestRow request(Connection c, UUID id, boolean lock) throws SQLException {
        Optional<RequestRow> row = lock ? store.lock(c, id) : store.load(c, id);
        return row.orElseThrow(() -> new IllegalArgumentException("unknown decision request " + id));
    }

    private DecisionView view(Connection c, RequestRow row) throws SQLException {
        String itemType = null;
        String businessKey = null;
        if (row.subjectType() == SubjectType.ITEM) {
            ItemRow item = ctx.items().load(c, row.subjectId());
            itemType = item.itemType();
            businessKey = item.businessKey();
        } else if (row.subjectType() == SubjectType.WORK_PACKAGE) {
            PackageRow pkg = new PackageStore(ctx.dialect()).load(c, row.subjectId());
            itemType = pkg.itemType();
            businessKey = "work package " + pkg.seq() + (pkg.groupKey() == null ? "" : " (" + pkg.groupKey() + ")");
        } else if (row.subjectType() == SubjectType.WINDOW_OCCURRENCE) {
            businessKey = new WindowStore().load(c, row.subjectId()).map(o -> "window " + o.window() + " #" + o.seq())
                    .orElse(null);
        } else if (row.subjectType() == SubjectType.RUN) {
            businessKey = runKey(c, row.subjectId());
        }
        List<VoteView> votes = store.votes(c, row.id()).stream()
                .map(v -> new VoteView(v.voter(), v.vote(), v.comment(), v.votedAt())).toList();
        return new DecisionView(row.id(), row.kind().name(), row.subjectType().name(), row.subjectId(), itemType,
                businessKey, ctx.catalog().byId(row.requestedByJobId()).name(), row.reason(), optionViews(row),
                row.requiredApprovals(), row.responsibleGroup(), row.state(), votes, row.decidedOption(),
                row.decidedBy(), row.decidedAt(), row.comment(), row.createdAt());
    }

    private DecisionRequest event(Connection c, RequestRow row) throws SQLException {
        String itemType = null;
        String businessKey = null;
        if (row.subjectType() == SubjectType.ITEM) {
            ItemRow item = ctx.items().load(c, row.subjectId());
            itemType = item.itemType();
            businessKey = item.businessKey();
        } else if (row.subjectType() == SubjectType.WORK_PACKAGE) {
            PackageRow pkg = new PackageStore(ctx.dialect()).load(c, row.subjectId());
            itemType = pkg.itemType();
            businessKey = "work package " + pkg.seq() + (pkg.groupKey() == null ? "" : " (" + pkg.groupKey() + ")");
        } else if (row.subjectType() == SubjectType.WINDOW_OCCURRENCE) {
            businessKey = new WindowStore().load(c, row.subjectId()).map(o -> "window " + o.window() + " #" + o.seq())
                    .orElse(null);
        } else if (row.subjectType() == SubjectType.RUN) {
            businessKey = runKey(c, row.subjectId());
        }
        int approvals = (int) store.votes(c, row.id()).stream().filter(v -> Vote.APPROVE.name().equals(v.vote()))
                                   .count();
        return new DecisionRequest(row.id(), DecisionRequest.Kind.valueOf(row.kind().name()),
                row.subjectType().name(), row.subjectId(), itemType, businessKey,
                ctx.catalog().byId(row.requestedByJobId()).name(), row.reason(), row.requiredApprovals(), approvals,
                row.responsibleGroup(), row.createdAt());
    }

    /** A run of a flow instance is shown with the instance, e.g. {@code end-of-day 2026-11-18}. */
    private String runKey(Connection c, UUID runId) throws SQLException {
        return new FlowInstanceStore(ctx.dialect()).ofRun(c, runId).map(i -> i.flowName() + " " + i.businessKey())
                                                  .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private List<OptionView> optionViews(RequestRow row) {
        List<Map<String, Object>> raw = ctx.json().read(row.options(), List.class);
        List<OptionView> options = new ArrayList<>();
        for (Map<String, Object> option : raw) {
            options.add(new OptionView(String.valueOf(option.get("name")), String.valueOf(option.get("label")),
                    Boolean.TRUE.equals(option.get("builtIn"))));
        }
        return options;
    }

    private static OptionView builtIn(BuiltInOption option) {
        return new OptionView(option.name(), option.label(), true);
    }

    private static String suffix(String comment) {
        return comment == null || comment.isBlank() ? "" : ": " + comment;
    }

    private <T> T inTx(Transactions.ConnectionWork<T> work) {
        return ctx.tx().required(() -> ctx.tx().withConnection(work));
    }
}
