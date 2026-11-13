# Mission: Distributed Processing Framework (PoC)

> Status: **v0.5** – decisions from the Q&A rounds are recorded in [§13 Decision Log](#13-decision-log).
> Items marked **[A]** are assumptions that still need confirmation, **[Q-n]** refers to [§14 Open Questions](#14-open-questions).
> Names marked with `*` are **working names**. The final terms are chosen in the DDD-based design (see §5.10).

---

## 1. Vision

A lightweight, **DB-backed workflow engine** for CDI developers. It coordinates **processing across a cluster** so
that every planned run happens **exactly once**: no duplicate or parallel runs of the same job, even when many nodes are
available. It survives node crashes and hangs, and it makes the path of every processed **data item** fully traceable,
including items that are routed back to earlier stages.

Most job runners **run continuously** and process incoming data as it arrives. On top of that, the framework supports
**time-bound processing**:
- **End-of-day (EOD) processing** of complex, interdependent business processes,
- **consistency windows**, in which a single runner collects a consistent data set (e.g. data packages forwarded to
  other systems as XML) without overlap and without double forwarding,
- **time and volume limits**, so that work exceeding a time slot is carried over to the next slot,
- **human decisions**: when automation cannot resolve a situation, a person of the business team picks from offered options.

The framework is meant to be the *backbone of large applications*. **Correctness, performance, type safety,
transparency and code quality** are first-class goals. At the same time it must stay **simple for small use cases**:
advanced concepts are opt-in.

## 2. Goals

| #   | Goal                         | Description |
|-----|------------------------------|-------------|
| G1  | Cluster-safe execution       | 1 job definition + 1 trigger occurrence → exactly 1 run. A job definition never has runs executing in parallel on several nodes. |
| G2  | Zero extra infrastructure    | Default coordination uses only the database (leases via `SELECT … FOR UPDATE SKIP LOCKED` / conditional updates). |
| G3  | Pluggable coordination       | Coordination is a port (SPI). Other implementations (K8s Lease, Redis, ZooKeeper/etcd, …) can be added later. |
| G4  | Failure detection & takeover | Hanging or crashed nodes are detected (lease expiry + heartbeat), and another node takes over according to a per-job policy. Zombie writes are prevented (fencing). |
| G5  | Active by default            | Adding the dependency to a deployment activates the engine. Multiple deployments join the cluster automatically. |
| G6  | Flexible triggering          | ASAP (continuous), cron (calendar-aware), external (REST), successor (status-dependent), item inbox and **fan-in/join**. |
| G7  | Item tracking & routing      | Items (referenced by type-safe `ItemRef`s) are routed forward, **backward** and sideways until they reach a final state, across days if necessary. |
| G8  | Transparency                 | A complete audit trail of runs, flow instances, packages, decisions, windows, nodes and every item's journey. |
| G9  | Type-safe, minimal API       | Trivial cases need a few lines of CDI code. Complex flows are possible through opt-in concepts and SPIs. The API avoids stringly-typed calls wherever possible. |
| G10 | Portable                     | Quarkus and MicroProfile runtimes, using MicroProfile/Jakarta APIs only (CDI, JAX-RS, JPA, JTA, MP Config/Health/Telemetry/OpenAPI/JWT). |
| G11 | Zero-downtime friendly       | Works with **red/green (blue/green) deployments**: two application versions running side by side against the same DB. |
| G12 | Quality                      | Hexagonal architecture + DDD enforced with ArchUnit, Checkstyle in the build, cluster/chaos and load tests. |
| G13 | Human-in-the-loop            | Situations that need a business decision create **decision requests** with options a business user can pick from. The decision drives the further flow. |
| G14 | Time windows & limits        | Consistency windows (exclusive data collection), time slots, time limits and item limits, with carry-over of remaining work to the next slot. |

## 3. Non-Goals (PoC)

- Distributing a *single* run's internal work across nodes. The runner holding the lease coordinates internal
  parallelism itself (virtual threads, partitioning, …).
- A general-purpose BPMN engine or a visual designer.
- A mandatory message broker (Kafka/JMS may become optional adapters later).
- Storing item payloads. The framework stores references + tracking metadata only.
- A web UI. Monitoring is provided through MicroProfile APIs and REST.
- Sub-second scheduling precision.
- Fine-grained **checkpoints** for resuming a run (postponed; work packages give package-level resumption, see §5.6).
- Querying **external calendar data** or depending on a calendar framework. The business calendar is a small, built-in concept.

## 4. Core Principles

1. **The DB is the single source of truth**: for definitions, runs, flow instances, items, packages, decisions,
   windows, calendars and cluster membership.
2. **Type safety first.** Strings and raw ids appear only at the boundaries (DB, REST). In Java code:
   - jobs are referenced by **handler class literals**,
   - parameters, result statuses and decision options are typed (`JobHandler<P, R extends ResultStatus>`, enums),
   - items are referenced by **`ItemRef<E, ID>`** (entity class + id + optional version), never by a bare `String`/`long`,
   - custom run info uses typed keys (`RunInfoKey<T>`),
   - configuration is bound to typed objects (MP Config `@ConfigProperties`).
3. **Simple by default, powerful on demand.** Each level of complexity is opt-in, and a small use case never has to
   deal with concepts it doesn't use:

   | Level | What a user needs | Concepts involved |
   |-------|-------------------|-------------------|
   | 1 – Basic | A handler class + a definition row | Job definition, run, triggers (ASAP/cron/external/successor) |
   | 2 – Items | + item tracking and routing | Items, inbox, journey, back-routing, loop limits |
   | 3 – Advanced | + whatever the process needs | Flow instances, joins, work packages, time slots and limits, consistency windows, decision requests, calendars |

4. **Convention over configuration**: sensible defaults, active by default, opt-out via MP Config.
5. **Minimal API surface, pluggable internals**: everything behind ports/SPIs with a default implementation.
6. **Never hold DB transactions or locks during job execution.** Use leases + heartbeats + fencing tokens instead.
7. **Soft stops only.** Cancellation, slot ends and window starts never interrupt running work. They prevent new
   work from starting.
8. **Design for 100M+ items/EOD, prove 1M/EOD in the PoC** (see §11).

## 5. Domain Model (Ubiquitous Language)

All framework entities use a **pluggable id type** (default `UUID`, preferably time-ordered UUIDv7 for index
locality; `Long`/sequence as an alternative) provided by an `IdGenerator` SPI. All tables carry a **`namespace`**
column. Nodes only see and claim data of their own namespace, so several applications can share one schema.

> **Job vs. task.** It may make sense to split the model into *jobs* (what and when: definition, schedule, transitions)
> and *tasks* (units of execution: runs and packages). The DDD design decides this, under the constraint that the
> default model stays simple (see §5.10). This document uses "job" throughout.

### 5.1 Job Definition (Aggregate)
Lives **only in the DB**. It describes *what* runs, *when*, and *what comes next*.

- `id`, `namespace`, `name` (unique per namespace), `description`, `revision` (optimistic locking + history)
- `handlerClass` – FQCN of the CDI bean implementing `JobHandler<P, R>`
- **Trigger(s)**:
  - `ASAP` – **continuous**: a new run is created immediately after the previous one finishes. This is the most common mode.
  - `CRON` – cron expression + time zone + optional **business calendar** (see §5.8) + misfire policy
  - `EXTERNAL` – only via REST/programmatic API (with typed parameters)
  - `SUCCESSOR` – triggered by transitions of other jobs (incl. joins)
  - `INBOX` (optional, combinable) – run when the job's item inbox has pending items (batch size / debounce / max wait)
- **Transitions** – per result status or decision option: `on(R.SUCCESS) → [B, C]`, `on(PARTIAL*) → [Repair]`
- **Join** – `C` waits for *all* of `[A, B]` (optionally with required statuses) **within the same flow instance**.
  The join is evaluated atomically in the DB so that exactly one node creates the run of `C`.
- **`concurrencyPolicy`** (what happens if a trigger fires while a run is active), configurable, **default `SKIP`**:
  `SKIP` (recorded as a `SKIPPED` run) / `QUEUE_ONE` / `QUEUE_ALL`
- **`takeoverPolicy`** (owner node lost), configurable, **default `RERUN`**:
  - `RERUN` – another node restarts the run. Packages that are already done are not processed again (§5.6).
  - `ABANDON` – the run becomes `ABANDONED` and an operator resolves it.
  - `RESUME` with fine-grained checkpoints is **postponed**.
- **`maxItemVisits`** – limit on how often a single item may enter this job's inbox. Items over the limit are parked.
- **`retryPolicy`** – a configurable **maximum number of retries**, applied **per item or per item group (work
  package)**, optionally with backoff. When the maximum is reached, a **decision request** is created for the business
  team (see §5.7).
- **Time slots & limits** (optional, see §5.8):
  - `slots` – the periods in which the job may process (e.g. "business days 18:00–22:00"). Without slots: always.
  - `timeLimit` – maximum processing time per slot/run
  - `itemLimit` – maximum number of items (or packages) per slot, e.g. a maximum amount of data forwarded per day
  - Work that is not finished when a limit is hit is **carried over to the next slot**.
- **`windowParticipation`** (optional) – whether this job is the **collector** of a consistency window, or is **paused**
  during it (see §5.9)
- `timeout`, `leaseDuration`, `enabled`, default parameters (JSON, mapped to `P`)

### 5.2 Flow Instance (Aggregate)
A first-class grouping of one end-to-end process execution, e.g. *"EOD for business date 2026-11-13"*.

- `id`, `namespace`, `flowName`, **`businessKey`** (e.g. the business date), status, start/end, custom info
- Created by a root trigger (cron/external). Successor runs inherit it.
- Guarantees idempotency per business key (`unique(namespace, flowName, businessKey)`).
- Provides the scope for joins and answers questions like "show me everything that happened for EOD X".
- Items are **not** bound to a single flow instance. They live until they reach a final state, and every journey entry
  records the flow instance (if any) in which it happened.

### 5.3 Job Run (Aggregate)
One execution of a job definition.

- `id`, `namespace`, `jobDefinitionId` + **definition snapshot/revision** (so later DB changes don't alter history)
- `flowInstanceId` (optional), `slot`/`window` (optional), `triggerType`, `triggeredBy` (cron slot, REST principal,
  predecessor run(s), decision)
- **`runKey`** – idempotency key (e.g. `definition + cron slot`, `definition + flow instance`, `definition + window`)
  with a unique constraint
- **Status lifecycle** (working names):
  `PENDING → CLAIMED → RUNNING → SUCCEEDED | PARTIAL* | CARRIED_OVER* | AWAITING_DECISION* | FAILED | CANCELLED | ABANDONED | SKIPPED`
  - `PARTIAL*` – completed, but at least one item failed or was parked. Item failures never stop a run.
  - `CARRIED_OVER*` – a time/item limit or the slot end was reached. Pending packages/items continue in the next slot.
  - `AWAITING_DECISION*` – blocked until a business user resolves a decision request (§5.7).
  - `CANCEL_REQUESTED*` → `CANCELLED` – **soft cancel**: work in progress (the current package) finishes, nothing new starts.
- `resultStatus` – the typed outcome `R` (stored as its name), which drives transitions
- `ownerNodeId`, `leaseUntil`, `lastHeartbeat`, **`fencingToken`**, `attempt`
- timestamps, error details (type, message, stack excerpt)
- **custom run info** – structured data written during the run through typed `RunInfoKey<T>`s, stored as JSON

### 5.4 Node (Cluster Member)
- `nodeId`, `namespace`, host, **deployment color/version**, `startedAt`, `lastHeartbeat`,
  state (`ACTIVE`, `DRAINING`, `DEAD`), **capabilities** (the handler classes available on this node)
- A node only claims runs whose handler it can execute. This supports red/green and heterogeneous deployments.

### 5.5 Tracked Item & Item Journey
**A stage is a job definition.** Each job definition owns an **inbox** of items.

- **`ItemRef<E, ID>`** – a type-safe value object: `entityClass`, `id` (pluggable type), optional `version`.
  The framework never stores payloads, only the reference to the application's own entity.
- `TrackedItem` – `id`, `namespace`, `itemRef`, `businessKey`, `currentJob` (inbox), `packageId` (optional), `state`
  (`PENDING`, `IN_PROGRESS`, `PARKED`, `AWAITING_DECISION*`, `COMPLETED`, `FAILED`), visit counters
- **Items live until they reach a final state.** Unresolved items stay in their inbox and are processed further by
  later runs (e.g. on the next day). Most runners work on their inboxes continuously.
- `ItemTransition` (append-only journal) – `item`, `fromJob`, `toJob`, `runId`, `packageId`, `flowInstanceId`, `nodeId`,
  `timestamp`, `outcome`, `reason`, optional diagnostic data
- Routing operations*: `routeTo(Job.class)`, **`routeBack(Job.class, reason)`**, `park(reason)`, `complete()`, `fail(reason)`
- Routing writes participate in the job's transaction (see §6.5), so item state and business data stay consistent.
- **Item failures are isolated.** A failed or parked item is recorded with its reason, the other items continue, and
  the run ends as `PARTIAL*`.
- **Loop protection.** When `maxItemVisits` of the target job is exceeded, the item is parked instead of being routed.
- The journal reconstructs the exact path of every item, including loops, back-routing, packages and decisions.

### 5.6 Work Package* (opt-in)
A unit of work that a runner creates from its input, e.g. **1000 items per package**.

- `id`, `namespace`, `runId` (creating run), `jobDefinitionId`, `sequence`, `itemCount`, `state`
  (`PENDING`, `IN_PROGRESS`, `DONE`, `FAILED`, `AWAITING_DECISION*`), `attempts`, `slot`/`window`, timestamps,
  custom info (e.g. the generated file name or external delivery id)
- **Optional acknowledgement**: when a package is forwarded to another system, the job can track its acknowledgement
  (`SENT` → `ACKNOWLEDGED` | `REJECTED`). A rejection counts as a failed attempt (retry policy, then a decision request).
  Without acknowledgement tracking, a package ends with `DONE` once it is sent.
- A package is processed **as a whole**. Once started it is finished, even if the slot ends or a cancel is requested (soft stop).
- When a slot ends, or a time or item limit is reached, packages that are created but still **pending wait for the
  next slot** of that job. They are neither dropped nor re-created.
- Packages are the resumption granularity: after a takeover (`RERUN`) or in the next slot, only pending/failed
  packages are processed. This covers most resumption needs without fine-grained checkpoints.
- Items are linked to their package, so the journey shows exactly which package (e.g. which XML file) an item left with.

### 5.7 Decision Request* (Human-in-the-loop, opt-in)
Created when automation cannot resolve a situation: an item or package reached its configured maximum number of
retries, or a handler explicitly asks for a decision.

- `id`, `namespace`, subject (`run` / `package` / `item(s)`), `reason`, context data (for the business user),
  **offered options**, `state` (`OPEN`, `DECIDED`), `decidedBy`, `decidedAt`, `chosenOption`, `comment`
- **Options are typed** (an enum implementing `DecisionOption`). The framework offers built-in options (`RETRY`,
  `SKIP`, `CANCEL`), and handlers can offer **custom options** (e.g. `ACCEPT_WITH_DEFAULT_PRICE`, `ROUTE_TO_MANUAL_REVIEW`).
- The chosen option drives the further flow through the **same transition mechanism** as result statuses
  (e.g. `on(ImportDecision.ACCEPT_WITH_DEFAULT_PRICE) → [BookTrades]`) or through a typed callback in the handler.
- Decisions are made via REST by users with a business role. They are fully audited (who, when, which option, comment).
- A CDI event (`DecisionRequested`) lets the application notify people (mail, chat, …). The framework itself doesn't send notifications.
- **Unanswered requests: wait by default.** An open request never expires and is never decided automatically. Only
  the affected subject (item/package/run) waits, and everything else continues.
- **Optional fallback job/task**: per job definition, a fallback handler can be configured together with a delay.
  If a request is still open after the delay, the framework triggers the fallback (a regular, cluster-safe run) with the
  open request as typed context, e.g. to send a reminder or escalate to someone else. The request stays open.
- **Decisions respect time slots.** When a decision is made after the job's slot has ended, the resulting work (e.g.
  "resend package 17") is processed in the **next slot**.

### 5.8 Business Calendar & Time Slots (opt-in)
- **Business calendar** – a named calendar per namespace, stored in the DB and maintained via REST/SQL: business days,
  non-business days (holidays) and time zone. There are no external data sources and no calendar framework.
- **Cron triggers** can reference a calendar. On non-business days they skip, or shift to the next or previous business
  day (policy per definition).
- **Time slot*** – a recurring, calendar-aware period in which a job may process, e.g. "business days 18:00–22:00".
  At the slot end, running packages finish, no new ones start, and the run ends as `CARRIED_OVER*`.
- **Limits** – `timeLimit` and `itemLimit` per slot. Example: at most N items forwarded per day. Whatever exceeds the
  limit waits for the next slot. Example: too much data arrived today → the remaining packages are processed tomorrow.

### 5.9 Consistency Window* (optional)
A recurring period in which **one collector job** gathers a **consistent data set**, e.g. the customer data
forwarded to another system for that day. This concept is **optional**. Using the framework does not require it.

- Windows are **defined via configuration**: `name`, `schedule` (calendar-aware), `collectorJob`, **`pausedJobs`**,
  `quiesceTimeout`. Like job definitions, they are stored **in the DB** and provisioned via SQL/Flyway or REST, so all
  nodes and app versions (red/green) see the same definition.
- **The framework's responsibility is limited to its own jobs/tasks.** It guarantees that the paused jobs don't run
  while the window is open. Writes from outside the framework (e.g. user edits via UI or REST) are the application's
  responsibility.
- **Each occurrence has a sequence number and boundaries** (`from` = end of the previous occurrence, `to` = cut-off),
  which the collector can use to select its data:
  - Changes made after a window are forwarded in the next occurrence, so occurrences don't overlap.
  - The occurrence id is part of the collector run's `runKey`, so a window is collected **at most once**, even across
    takeovers and red/green switches.
- **Lifecycle of an occurrence**:
  1. `QUIESCING` – no new runs/packages of the paused jobs start (soft stop). Running packages finish.
  2. `OPEN` – once the paused jobs are idle, the framework records the cut-off and starts the collector run.
     The collector creates packages (e.g. XML files) and records which items/entities went into which package.
  3. `CLOSED` – the paused jobs resume and process everything that arrived in the meantime.
  4. If quiescing exceeds `quiesceTimeout`, or the collector fails, a **decision request** is created.
- Continuous runners therefore run **all the time except during the windows they are paused for**.

### 5.10 Terms to Define During the Design
Following DDD, the ubiquitous language is settled during the design, not by these working names:
- *job* vs. *task* (definition / run / package / handler / "task runner")
- run statuses: `PARTIAL*`, `CARRIED_OVER*`, `AWAITING_DECISION*`, `CANCEL_REQUESTED*`
- *work package*, *decision request* / *decision option*, *time slot*, *consistency window* / *cut-off*
- item states and routing verbs (`routeTo`, `routeBack`, `park`, …)

## 6. Coordination Concept

### 6.1 Default: DB-based (no extra infrastructure)
1. **Node registry & heartbeat** – each node registers itself on startup and heartbeats periodically. A node whose
   heartbeat is stale is considered `DEAD`.
2. **Trigger materialization** – every node computes due triggers (cron, ASAP, inbox, transitions, joins, decisions,
   slot starts, window occurrences) and inserts `PENDING` runs. The unique `runKey` makes concurrent inserts by several
   nodes harmless (insert-or-ignore).
3. **Claiming** – nodes poll with `SELECT … FOR UPDATE SKIP LOCKED` (PostgreSQL) or a dialect-specific equivalent,
   and set `owner`, `leaseUntil` and an incremented `fencingToken` in a short transaction.
4. **Single active run per definition** – enforced by the DB, e.g. by a partial unique index on active runs per
   definition or a per-definition lock row. This holds no matter how many nodes exist.
5. **Lease renewal** – the owner extends `leaseUntil` periodically. No transaction stays open during execution.
6. **Takeover** – runs with an expired lease are handled according to `takeoverPolicy`.
7. **Fencing** – every runner write (status, run info, item routing, package state) is conditional on its
   `fencingToken`. A zombie node that lost its lease cannot overwrite state.
8. **Graceful shutdown** – the node switches to `DRAINING`, stops claiming, finishes or releases its leases, and deregisters.

### 6.2 Soft Stops (cancel, slot end, window start, drain)
All "stop" signals work the same way: a flag in the DB that runners check **between units of work** (package, batch,
item), exposed to handlers as `ctx.shouldContinue()`. The framework stops handing out new packages and inbox batches
itself. Threads are never interrupted.

### 6.3 Pluggable
- `CoordinationProvider` / `LeaseManager` port, with the JDBC implementation as the default.
- `SqlDialect` SPI. **PostgreSQL** and **H2** (in-memory tests and dev) come in the PoC. Others (Oracle, SQL Server,
  MySQL, DB2) can be added later as adapters without core changes.
- Selection via MP Config / CDI alternatives.

### 6.4 Red/Green Deployments
- Definitions in the DB are independent of the code version. Each run stores the definition snapshot it was created with.
- Nodes advertise **color/version + capabilities**. Runs are only claimed by nodes that have the handler.
- An **activation switch** per namespace (DB/REST) lets the new color take over new runs while the old color drains.
  Takeover rules still apply if the old color is killed hard. Who flips the switch is defined later **[Q-1]**.
- Because different app versions may run at the same time, definitions must **not** be provisioned by code at startup
  (e.g. the builder API). In such setups definitions come from SQL/Flyway or the REST admin API only.
- Framework schema migrations (Flyway) follow **expand/contract**, so old and new versions can run against the same schema.

### 6.5 Transactions
- Framework tables live in the **same datasource as the business data by default**. Item routing, package state and
  run-info writes join the job's JTA/JPA transaction, so they commit atomically with business changes.
- A separate datasource is configurable. Its weaker guarantees are documented (idempotent handlers required).

### 6.6 Defaults & Zero Configuration
- The engine starts automatically on application startup (opt-out: `dp.enabled=false`).
- Polling intervals, lease durations, heartbeat intervals and thread pools (virtual threads by default) all have
  defaults and can be overridden via MP Config.

## 7. Job Definition Provisioning

The DB is the only source of truth. There are several ways to write to it:

| Mechanism | Main use |
|-----------|----------|
| **SQL / Flyway scripts** shipped by the application | Production: versioned, reviewable, red/green-safe |
| **REST admin API** (JAX-RS, secured) | Operations: create, update, enable/disable, change cron/slots/limits, maintain calendars and windows, inspect history |
| **Type-safe builder API** (optional) | Smaller apps and tests only. **Not suitable** when several app versions run at the same time (red/green), see §6.4 |

On startup, every node **validates** the enabled definitions of its namespace against its capabilities. It reports
unknown handler classes and type mismatches (parameters, result statuses, decision options) via logs and health checks.
Definitions of other colors/versions are not treated as errors.

## 8. Developer API (Sketch)

### 8.1 Level 1 – a simple job handler
```java
@ApplicationScoped
public class CleanupTempFiles implements JobHandler<NoParams, DefaultOutcome> {
    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        // business logic …
        return JobResult.success();
    }
}
```

### 8.2 Level 2 – tracking and routing items
```java
public enum ImportOutcome implements ResultStatus { SUCCESS, NO_DATA }

@ApplicationScoped
public class ImportTrades implements JobHandler<ImportParams, ImportOutcome> {

    public static final RunInfoKey<Long> IMPORTED = RunInfoKey.of("importedRecords", Long.class);

    @Inject TradeImporter importer;

    @Override
    public JobResult<ImportOutcome> execute(JobContext<ImportParams> ctx) {
        List<Trade> trades = importer.importFor(ctx.parameters().businessDate());
        trades.forEach(t -> ctx.items().track(ItemRef.of(Trade.class, t.getId(), t.getVersion()))
                                       .routeTo(ValidateTrades.class));
        ctx.runInfo().put(IMPORTED, (long) trades.size());
        return JobResult.of(trades.isEmpty() ? ImportOutcome.NO_DATA : ImportOutcome.SUCCESS);
    }
}

@ApplicationScoped
public class ValidateTrades implements JobHandler<NoParams, DefaultOutcome> {

    @Inject EntityManager em;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        ctx.inbox(Trade.class, UUID.class).forEachBatch(500, batch -> {   // stops softly on cancel/slot end
            for (TrackedItem<Trade, UUID> item : batch) {
                Trade trade = em.find(Trade.class, item.ref().id());
                if (isValid(trade)) {
                    item.routeTo(EnrichTrades.class);
                } else {
                    item.routeBack(ImportTrades.class, "missing ISIN");
                }
            }
        });
        return JobResult.success();   // becomes PARTIAL* automatically if items failed/were parked
    }
}
```

### 8.3 Level 3 – packages, limits and a decision request
```java
public enum ExportDecision implements DecisionOption { RESEND_PACKAGE, SKIP_PACKAGE, ROUTE_TO_MANUAL_REVIEW }

@ApplicationScoped
public class ExportCustomers implements JobHandler<NoParams, DefaultOutcome> {   // collector of a consistency window

    @Inject CustomerXmlWriter writer;
    @Inject PartnerGateway gateway;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        Window window = ctx.window().orElseThrow();      // occurrence n with cut-off(n-1) .. cut-off(n)

        ctx.packages().createFrom(changedCustomers(window.from(), window.cutOff()), 1000);

        ctx.packages().forEachPending(pkg -> {            // stops softly at the slot end/limit → CARRIED_OVER*
            try {
                gateway.send(writer.toXml(pkg.items()));
                pkg.done();
            } catch (PartnerUnavailableException e) {
                pkg.requestDecision("Partner rejected package " + pkg.sequence(), ExportDecision.class);
            }
        });
        return JobResult.success();
    }
}
```

### 8.4 Type-safe definition builder (optional; small apps and tests)
```java
definitions.define(ImportTrades.class)
    .namespace("eod")
    .cron("0 0 22 * * ?", ZoneId.of("Europe/Vienna")).calendar("AT-BANKING")
    .on(ImportOutcome.SUCCESS).trigger(ValidateTrades.class)
    .save();

definitions.define(BookTrades.class)
    .namespace("eod")
    .join(ValidateTrades.class, ImportFxRates.class)     // fan-in within the same flow instance
    .save();

definitions.define(ExportCustomers.class)
    .namespace("eod")
    .slot(BusinessDays.of("AT-BANKING"), LocalTime.of(18, 0), LocalTime.of(22, 0))
    .itemLimit(500_000)
    .on(ExportDecision.ROUTE_TO_MANUAL_REVIEW).trigger(ManualReview.class)
    .save();

windows.define("customer-export")
    .collector(ExportCustomers.class)
    .pausing(UpdateCustomers.class, ImportCustomerChanges.class)
    .schedule(BusinessDays.of("AT-BANKING"), LocalTime.of(18, 0))
    .save();
```

### 8.5 Extension points
- CDI events (sync/async): `RunStarted`, `RunFinished`, `ItemRouted`, `PackageCompleted`, `DecisionRequested`,
  `DecisionMade`, `WindowOpened`, `WindowClosed`, `NodeJoined`, `NodeLost`, `FlowInstanceCompleted`, …
- Interceptors/decorators around job execution (e.g. MDC, security context, custom metrics)
- SPIs: `CoordinationProvider`, `SqlDialect`, `IdGenerator`, `RunInfoCodec`, `ParameterCodec`, `Clock`, `CronParser`
- Programmatic API: `JobEngine` (trigger, cancel, retry), queries for runs, items, packages, decisions and flow instances

## 9. Architecture

### 9.1 Hexagonal + DDD
```
        ┌──────────────── inbound adapters ────────────────┐
        │ CDI bootstrap · JAX-RS · scheduler/poller loop   │
        └────────────────────────┬─────────────────────────┘
                                 ▼
                 application (use cases, in/out ports)
                                 ▼
             domain (aggregates, value objects, policies,
                     domain events — plain Java only)
                                 ▲
        ┌────────────────────────┴─────────────────────────┐
        │ JPA persistence · JDBC coordination · dialects ·  │
        │ MP Health · MP Telemetry · Flyway   (outbound)   │
        └──────────────────────────────────────────────────┘
```

Candidate bounded contexts / sub-domains **[A]**: *Scheduling* (definitions, triggers, calendars, slots, windows),
*Execution & Coordination* (runs, leases, nodes, packages), *Item Tracking* (items, routing, journey),
*Decisions* (decision requests, options). The design decides whether they become separate modules or packages.

### 9.2 Module layout **[A]**
| Module | Content | Allowed dependencies |
|--------|---------|----------------------|
| `dp-api` | User API: `JobHandler`, `JobContext`, `JobResult`, `ResultStatus`, `DecisionOption`, `ItemRef`, `RunInfoKey`, builder | JDK, CDI API |
| `dp-spi` | Extension SPIs | `dp-api` |
| `dp-domain` | Aggregates, value objects, domain services, events | JDK only |
| `dp-application` | Use cases, inbound/outbound ports | `dp-domain`, `dp-spi` |
| `dp-adapter-persistence-jpa` | JPA entities, repositories | `dp-application`, JPA, JTA |
| `dp-adapter-coordination-jdbc` | Lease/claim/fencing, `SqlDialect` (PostgreSQL, H2) | `dp-application`, JDBC |
| `dp-adapter-schema-flyway` | Versioned migrations per dialect | Flyway |
| `dp-adapter-rest` | JAX-RS admin/trigger/inspection/decision API, MP OpenAPI, MP JWT roles | `dp-application` |
| `dp-adapter-scheduler` | Cron + calendar evaluation, slots, windows, trigger materialization, poll loop | `dp-application` |
| `dp-adapter-observability` | MP Health, MP Telemetry (traces + metrics) | `dp-application` |
| `dp-cdi` | CDI integration via **Build Compatible Extensions** (works in CDI Lite/Quarkus ArC and full CDI) | adapters |
| `dp-quarkus` (runtime + deployment) | Quarkus extension (build-time processing, native readiness later) | adapters |
| `dp-architecture-tests` | ArchUnit rules | all |
| `dp-examples` | Samples per level: simple job, item routing with back-routing, EOD flow with join, consistency-window export with packages, limits and a decision | `dp-api` |

### 9.3 Platform
- **Java 25**, **Maven** multi-module build
- **Quarkus** as the primary target
- **Helidon MP** and **Apache Meecrowave** as secondary targets, **only if** they support Java 25 and the same
  Jakarta EE / MicroProfile API level. This is checked in a spike at the start of the PoC (Meecrowave's release
  activity and API level are a known risk).
- Only standard APIs are used: CDI 4.x, Jakarta REST, JPA, JTA, JSON-B/JSON-P, MP Config, MP Health,
  MP Telemetry, MP OpenAPI, MP JWT.

### 9.4 Quality gates
- **Checkstyle** with a shared config; the build fails on violations
- **ArchUnit**: layering (the domain is free of framework dependencies), no cycles, adapters don't depend on each other,
  the public API exposes no implementation types, naming conventions
- Unit tests (domain), integration tests with **Testcontainers (PostgreSQL)** and **H2**
- **Cluster/chaos tests**: N engine instances against one DB, killed/frozen nodes, clock skew, asserting no duplicates,
  correct takeover, fencing, and joins triggered exactly once
- **Window tests**: paused jobs never run while a window is open, consecutive occurrences have contiguous boundaries,
  and each occurrence is collected at most once, incl. takeover of the collector
- **Limit/slot tests**: carry-over of pending packages to the next slot; the soft stop never interrupts a running package
- **Red/green test**: two "colors" with different capabilities against one DB
- **Load tests** against the targets in §11

## 10. Success Criteria for the PoC **[A]**
1. N nodes (≥ 3) against one DB execute the sample flows, and every run happens exactly once.
2. Killing/freezing the owner node → another node takes over within `leaseDuration + pollInterval`. Done packages are
   not processed again.
3. A resumed "zombie" node cannot corrupt state (fencing verified by test).
4. Items are routed forward and backward, failing items lead to `PARTIAL*`, and the full journey is visible via REST.
5. A consistency window over several simulated days: paused jobs never run during it, and each occurrence is collected exactly once.
6. Slot ends and item limits carry pending packages over to the next slot.
7. A decision request is created, decided via REST, and the chosen option drives the next transition.
8. The same example application runs on Quarkus (and on Helidon/Meecrowave if the spike is positive).
9. ArchUnit + Checkstyle are enforced in the build, and the 1M items/EOD target is met.

## 11. Non-Functional Targets

| Aspect | PoC target | Design must not block |
|--------|-----------|------------------------|
| Items per EOD | ~1M | 100M+ |
| Job definitions | tens | hundreds |
| Nodes | 2–5 | 20+ |
| Failover (lease expiry → takeover) | < 1 min (configurable) | – |

Design consequences:
- Item routing, package and journey writes are **batched** (JDBC batch, no per-item round trips or transactions).
- The journey table is append-only and partition-ready (e.g. by date).
- **Retention:** all data stays in place and queryable. There is **no auto-cleanup or archiving** for now. The schema
  must not block adding them later (e.g. by dropping partitions).
- Inbox and package processing uses keyset pagination and no `OFFSET`.
- Indexes are designed for claim queries and lookups (by `itemRef`, `businessKey`, `flowInstanceId`, `packageId`).

## 12. Observability & Operations
Only MicroProfile/Jakarta APIs are used. There is no UI in the PoC.

- **MP Health** – liveness/readiness, coordination status, DB reachability, definition validation result
- **MP Telemetry** – traces (span per run/package, correlated by flow instance/run id) and metrics (runs by status,
  durations, claim lag, inbox depth per job, items routed/back-routed/parked, pending/carried-over packages,
  open decision requests and their age, window state and duration, lease renewals/takeovers)
- **MP OpenAPI** – documented REST API
- **REST API** (Jakarta REST, secured via **MP JWT + `@RolesAllowed`**: `dp-viewer`, `dp-operator`, `dp-business`,
  `dp-admin`; security can be switched off for dev/tests):
  - definitions: CRUD, enable/disable, revision history
  - calendars, slots, consistency windows: maintain, inspect occurrences and cut-offs
  - flow instances: list, inspect, start (external trigger)
  - runs: list/filter, inspect (incl. run info), trigger, **soft cancel**, retry, resolve `ABANDONED`
  - packages: list per run/slot/window, inspect contained items
  - **decision requests**: list open ones, inspect context, **decide** (pick an option + comment) – role `dp-business`
  - items: lookup by `ItemRef`/business key, **full journey**, re-route/park/release
  - nodes: cluster view, drain, activation switch (red/green)
- Structured logging with MDC (`namespace`, `flowInstanceId`, `runId`, `packageId`, `nodeId`)

## 13. Decision Log

| # | Topic | Decision |
|---|-------|----------|
| D1 | Source of truth for definitions | **DB only.** Provisioning via SQL/Flyway and REST; the type-safe builder is optional (small apps/tests) |
| D2 | Handler binding | Handler **FQCN** in the DB. Typed `JobType<P, R>` keys are left to the detailed design (not required for the PoC) |
| D3 | Stages | **Stage = job definition** (each job has an item inbox) |
| D4 | Item data | Metadata + reference only. Items are identified by a type-safe **`ItemRef<E, ID>`** (entity class, id, optional version) |
| D5 | Id type | Default **UUID**, pluggable (e.g. `Long`) through an SPI |
| D6 | Takeover after node loss | Configurable per job: `RERUN` (default) / `ABANDON`. `RESUME` with checkpoints is postponed |
| D7 | Inbox-triggered runs | Supported, opt-in per job (batch size/debounce) |
| D8 | Concurrency policy | Configurable per job, **default `SKIP`** (recorded as a `SKIPPED` run) |
| D9 | Flow instances | First-class aggregate with a business key |
| D10 | ASAP semantics | **Continuous** (re-run after finish). This is the most common mode |
| D11 | Type safety | As much of the API as possible is type-safe (class literals, typed keys, generics, enums, `ItemRef`) |
| D12 | Databases | **PostgreSQL + H2** in the PoC; dialects are pluggable |
| D13 | Build/Java | **Maven, Java 25** |
| D14 | Runtimes | Quarkus. Helidon MP and Apache Meecrowave only if they support Java 25 and the same API level |
| D15 | Volumes | 1M items/EOD for the PoC, without blocking 100M+ |
| D16 | Schema migration | **Flyway** |
| D17 | Monitoring | MicroProfile APIs (Health, Telemetry, OpenAPI) + REST, no UI |
| D18 | Multi-application | A **`namespace` column** on all tables |
| D19 | Fan-in / join | **In the PoC scope** |
| D20 | Transactions | Same datasource as business data by default (atomic routing); a separate datasource is configurable |
| D21 | Deployments | Must support **red/green deployments** |
| D22 | REST security | **MP JWT + roles** |
| D23 | Definitions with several app versions running | No code-based provisioning (builder API) when several app versions run at the same time. Use SQL/Flyway or REST |
| D24 | Item failures | Never stop the run. Other items continue, and the run ends as `PARTIAL*` |
| D25 | Back-routing loops | Configurable `maxItemVisits` limit per job definition. Items over the limit are parked |
| D26 | Retention | Keep everything. No auto-cleanup/archiving for now, but the schema must not block it later |
| D27 | Unresolvable failures | Create a **decision request** for the business team, with typed options to pick from. The chosen option drives the flow |
| D28 | Checkpoints | **Postponed.** Work packages provide package-level resumption |
| D29 | Consistency windows | One collector job runs exclusively, the paused jobs soft-stop, and cut-offs guarantee no overlap and no double forwarding |
| D30 | Item lifetime | Items stay in the system and are processed further (across days) until they reach a final state |
| D31 | Business calendar | Needed, but built in and simple (stored in the DB). No external data, no calendar framework |
| D32 | Cancellation | **Soft cancel**: running work finishes, nothing new starts |
| D33 | Limits & packages | Time limits, item limits and time slots per job. Runners create work packages, and pending packages wait for the next slot |
| D34 | Job vs. task | A possible split is decided in the DDD design. The default model must stay simple |
| D35 | Window scope | Windows are optional and defined via configuration. The framework only ensures that the paused jobs/tasks don't run during a window. Writes outside the framework are the application's responsibility |
| D36 | Retries | Configurable maximum number of retries **per item or per item group (package)**. When it is reached, a decision request is created |
| D37 | External acknowledgement | **Optional** per job. If enabled: `SENT` → `ACKNOWLEDGED` / `REJECTED`, and a rejection counts as a failed attempt |
| D38 | Unanswered decisions | Wait indefinitely by default. An optional fallback job/task (handler + delay) can be configured, e.g. to send notifications. The request stays open |
| D39 | Decisions vs. time slots | Work resulting from a decision made outside the slot waits for the **next slot** |
| D40 | Window configuration | Stored in the **DB**, like job definitions (SQL/Flyway or REST) |

## 14. Open Questions

### Deferred
- **Q-1** Red/green activation: who flips the switch, and what does the old color still finish? → *Defined later.*
- **Q-10** Typed `JobType<P, R>` keys in addition to class literals → *Detailed design, not required for the PoC.*

### Open
None at the moment. New questions will come up during the DDD-based design (see §5.10).

### Resolved
Q-2 → D27 · Q-3 → D28/D29 · Q-4 → D25 · Q-5 → D30 · Q-6 → D26 · Q-7 → D31 · Q-8 → D24 · Q-9 → D32 ·
Q-11 → D35 · Q-12 → D38 · Q-13 → D36 · Q-14 → D37 · Q-15 → D39
