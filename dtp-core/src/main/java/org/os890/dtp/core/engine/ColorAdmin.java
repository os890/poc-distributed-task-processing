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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.os890.dtp.api.event.ActiveColorSwitched;
import org.os890.dtp.api.event.ActiveColorUnavailable;
import org.os890.dtp.api.event.RunRef;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.jdbc.ColorStore.Control;
import org.os890.dtp.core.jdbc.ColorStore.SwitchRow;
import org.os890.dtp.core.jdbc.FlowStore.ActivationRow;
import org.os890.dtp.core.jdbc.FlowStore.FlowRow;
import org.os890.dtp.core.jdbc.FlowStore.VersionRow;
import org.os890.dtp.core.jdbc.NodeStore.ColorNode;

/**
 * The activation switch of red/green deployments (mission §6.4, Q-1, slice 21). The nodes of a release share a color
 * ({@code dtp.node.color}); an operator or the deployment pipeline switches the active color once the nodes of the new
 * release are up. From then on only nodes of the active color claim runs and lead; the nodes of the old color finish
 * the current unit of work of their runs (soft stop {@code DRAIN}), stand by and can be shut down. The switch
 * activates the flow versions that the nodes of the new color run, so switching back is the rollback. A color without
 * live node is noticed by the nodes on standby (slice 24): announced once, and optionally switched back to a color that
 * is live ({@code dtp.color.fallback-after}).
 */
public class ColorAdmin {

    private static final System.Logger LOG = System.getLogger(ColorAdmin.class.getName());

    private final EngineContext ctx;

    public ColorAdmin(EngineContext ctx) {
        this.ctx = ctx;
    }

    /**
     * The active color and the latest switches.
     *
     * @param activeColor the active color, or {@code null} if all colors claim
     * @param switchedAt  when it was switched, or {@code null} if never
     * @param switchedBy  who switched it, or {@code null}
     * @param reason      why, or {@code null}
     * @param switches    the latest switches, newest first
     * @param unavailableSince since when the active color has no live node, or {@code null}
     * @param draining    the runs that nodes of other colors still hold: once there are none, their nodes can be
     *                    shut down
     */
    public record ColorState(String activeColor, Instant switchedAt, String switchedBy, String reason,
                             List<SwitchView> switches, Instant unavailableSince, List<RunRef> draining) {
    }

    /**
     * A switch of the active color.
     *
     * @param previousColor  the color active before, or {@code null}
     * @param activeColor    the color active since, or {@code null}
     * @param switchedAt     when
     * @param switchedBy     who
     * @param reason         why, or {@code null}
     * @param activatedFlows the flow versions it activated ({@code "<flow> <hash>"})
     */
    public record SwitchView(String previousColor, String activeColor, Instant switchedAt, String switchedBy,
                             String reason, List<String> activatedFlows) {
    }

    /**
     * The result of a switch.
     *
     * @param changed        {@code false} if the color was active already (nothing happened)
     * @param previousColor  the color active before, or {@code null}
     * @param activeColor    the active color, or {@code null}
     * @param nodes          the live nodes of the active color
     * @param activatedFlows the flow versions activated ({@code "<flow> <hash>"})
     * @param warnings       what the operator should look at, e.g. jobs no node of the new color can run
     */
    public record ColorSwitch(boolean changed, String previousColor, String activeColor, List<String> nodes,
                              List<String> activatedFlows, List<String> warnings) {
    }

    /**
     * The active color and the latest switches.
     *
     * @param limit maximum number of switches
     * @return the state
     */
    public ColorState state(int limit) {
        return ctx.tx().withConnection(c -> {
            Optional<Control> control = ctx.colors().control(c, ctx.namespace(), false);
            List<SwitchView> switches = new ArrayList<>();
            for (SwitchRow row : ctx.colors().switches(c, ctx.namespace(), limit)) {
                switches.add(new SwitchView(row.previousColor(), row.activeColor(), row.switchedAt(),
                        row.switchedBy(), row.reason(), row.activatedFlows() == null ? List.of()
                        : List.of(ctx.json().read(row.activatedFlows(), String[].class))));
            }
            String active = control.map(Control::activeColor).orElse(null);
            List<RunRef> draining = new ArrayList<>();
            if (active != null) {
                for (UUID run : ctx.runs().heldByOtherColors(c, ctx.namespace(), active)) {
                    draining.add(ctx.runRef(c, run));
                }
            }
            return new ColorState(active, control.map(Control::switchedAt).orElse(null),
                    control.map(Control::switchedBy).orElse(null), control.map(Control::reason).orElse(null),
                    List.copyOf(switches), control.map(Control::unavailableSince).orElse(null),
                    List.copyOf(draining));
        });
    }

    /**
     * Switches the active color. Refused if the color has no live node (active or on standby, with a recent
     * heartbeat). Activates the flow versions the nodes of the color run, unless they run different versions of a
     * flow (a warning). Switching to the active color again changes nothing.
     *
     * @param color  the color, or {@code null}: all colors claim again (e.g. once only one release is left)
     * @param by     who (an operator or the deployment pipeline)
     * @param reason why, e.g. the release, or {@code null}
     * @return the result
     */
    public ColorSwitch activate(String color, String by, String reason) {
        Objects.requireNonNull(by, "by");
        if (color != null) {
            DtpConfig.requireColor(color);
        }
        ColorSwitch result = ctx.tx().required(() -> ctx.tx().withConnection(c -> switchTo(c, color, by, reason)));
        log(result, by);
        return result;
    }

    private static void log(ColorSwitch result, String by) {
        if (result.changed()) {
            LOG.log(System.Logger.Level.INFO, "active color switched from {0} to {1} by {2}, flows activated: {3}",
                    Objects.requireNonNullElse(result.previousColor(), "all colors"),
                    Objects.requireNonNullElse(result.activeColor(), "all colors"), by, result.activatedFlows());
            result.warnings().forEach(w -> LOG.log(System.Logger.Level.WARNING, "switch to {0}: {1}",
                    result.activeColor(), w));
        }
    }

    /**
     * Watches the active color after a heartbeat of this node, with the switch the heartbeat read (slice 24). A node of
     * the active color clears a mark of its color as unavailable. A node on standby notices that the active color has
     * no live node: the first one records it and publishes {@link ActiveColorUnavailable}. With
     * {@code dtp.color.fallback-after}, once the color is unavailable that long, it switches to its own color.
     *
     * @param control the switch as read by the heartbeat, or {@code null} if the namespace never had one
     */
    public void watch(Control control) {
        if (control == null || control.activeColor() == null) {
            return;
        }
        String active = control.activeColor();
        if (!NodeColor.standby(ctx.color().color(), active)) {
            if (control.unavailableSince() != null && ctx.tx().required(() -> ctx.tx().withConnection(c -> ctx.colors()
                    .markAvailable(c, ctx.namespace(), active)))) {
                LOG.log(System.Logger.Level.INFO, "color {0} has a live node again: {1}", active,
                        ctx.config().nodeName());
            }
            return;
        }
        if (control.unavailableSince() == null) {
            ctx.tx().required(() -> ctx.tx().withConnection(c -> noticeUnavailable(c, active)));
        } else if (ctx.config().colorFallbackAfter() != null) {
            fallback(active);
        }
    }

    private Object noticeUnavailable(Connection c, String active) throws SQLException {
        Instant now = ctx.clock().now(c);
        if (!ctx.nodes().liveNodes(c, ctx.namespace(), active, now.minus(ctx.config().deadAfter())).isEmpty()
                || !ctx.colors().markUnavailable(c, ctx.namespace(), active, now)) {
            return null;
        }
        Instant last = ctx.nodes().lastHeartbeat(c, ctx.namespace(), active);
        Duration after = ctx.config().colorFallbackAfter();
        LOG.log(System.Logger.Level.ERROR, "the active color {0} has no live node (last heartbeat {1}): nobody claims"
                + " runs or leads until the color is switched{2}", active, last, after == null ? ""
                : ", a node on standby switches after " + after);
        ctx.publishAfterCommit(new ActiveColorUnavailable(active, now, last, ctx.config().nodeName(), after));
        return null;
    }

    /**
     * Switches from an unavailable active color to the color of this node, once it is unavailable for
     * {@code dtp.color.fallback-after}. The switch is checked again under the lock: a node of the color that came
     * back, or another node that switched first, cancels it.
     *
     * @param unavailable the active color without live node
     * @return the switch, or empty if nothing was switched
     */
    public Optional<ColorSwitch> fallback(String unavailable) {
        Duration after = ctx.config().colorFallbackAfter();
        if (after == null) {
            return Optional.empty();
        }
        String by = "dtp (" + ctx.config().nodeName() + ")";
        ColorSwitch result = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Instant now = ctx.clock().now(c);
            Control current = ctx.colors().control(c, ctx.namespace(), true).orElse(null);
            if (current == null || !unavailable.equals(current.activeColor()) || current.unavailableSince() == null
                    || now.isBefore(current.unavailableSince().plus(after))
                    || !ctx.nodes().liveNodes(c, ctx.namespace(), unavailable, now.minus(ctx.config().deadAfter()))
                           .isEmpty()) {
                return null;
            }
            return switchTo(c, ctx.color().color(), by, "fallback: color " + unavailable + " has no live node since "
                    + current.unavailableSince());
        }));
        if (result == null) {
            return Optional.empty();
        }
        log(result, by);
        return Optional.of(result);
    }

    private ColorSwitch switchTo(Connection c, String color, String by, String reason) throws SQLException {
        Instant now = ctx.clock().now(c);
        // the lock serializes concurrent switches
        Optional<Control> current = ctx.colors().control(c, ctx.namespace(), true);
        String previous = current.map(Control::activeColor).orElse(null);
        List<ColorNode> nodes = color == null ? List.of()
                : ctx.nodes().liveNodes(c, ctx.namespace(), color, now.minus(ctx.config().deadAfter()));
        List<String> names = nodes.stream().map(ColorNode::name).toList();
        if (Objects.equals(previous, color)) {
            return new ColorSwitch(false, previous, color, names, List.of(), List.of());
        }
        if (color != null && nodes.isEmpty()) {
            throw new DtpException("color " + color + " has no live node, it can't be the active color");
        }
        List<String> warnings = new ArrayList<>();
        List<String> activated = new ArrayList<>();
        if (color != null) {
            activateFlows(c, color, nodes, by, reason, now, activated, warnings);
            warnings.addAll(jobsWithoutNode(color, nodes));
        }
        ctx.colors().switchTo(c, ctx.namespace(), previous, new Control(color, now, by, reason),
                ctx.json().write(activated));
        // the states follow at once; the heartbeats of the nodes keep them
        ctx.nodes().applyActiveColor(c, ctx.namespace(), color);
        ctx.publishAfterCommit(new ActiveColorSwitched(previous, color, by, reason, activated));
        return new ColorSwitch(true, previous, color, names, List.copyOf(activated), List.copyOf(warnings));
    }

    private void activateFlows(Connection c, String color, List<ColorNode> nodes, String by, String reason,
                               Instant now, List<String> activated, List<String> warnings) throws SQLException {
        Map<String, Set<String>> versions = new TreeMap<>();
        for (ColorNode node : nodes) {
            if (node.flowVersions() != null) {
                ctx.json().readMap(node.flowVersions()).forEach((flow, hash) ->
                        versions.computeIfAbsent(flow, f -> new TreeSet<>()).add(String.valueOf(hash)));
            }
        }
        for (Map.Entry<String, Set<String>> entry : versions.entrySet()) {
            String flow = entry.getKey();
            if (entry.getValue().size() > 1) {
                warnings.add("the nodes of color " + color + " run different versions of flow " + flow + " "
                        + entry.getValue() + ", none was activated");
                continue;
            }
            String hash = entry.getValue().iterator().next();
            Optional<FlowRow> row = ctx.flows().flow(c, ctx.namespace(), flow);
            Optional<VersionRow> version = row.isEmpty() ? Optional.empty()
                    : ctx.flows().version(c, row.get().id(), hash);
            if (version.isEmpty()) {
                // a node on standby doesn't register flows whose step jobs are missing: its nodes do once active
                warnings.add("flow " + flow + " has no version " + hash + " yet, the nodes of color " + color
                        + " register and activate it once active");
                continue;
            }
            UUID active = ctx.flows().activeVersion(c, row.get().id(), now).orElse(null);
            if (!version.get().id().equals(active)) {
                ctx.flows().activate(c, row.get().id(), new ActivationRow(version.get().id(), now, false, by,
                        "color " + color + " activated" + (reason == null ? "" : ": " + reason), now, null));
                activated.add(flow + " " + hash);
            }
        }
    }

    private List<String> jobsWithoutNode(String color, List<ColorNode> nodes) {
        Set<String> capable = new TreeSet<>();
        for (ColorNode node : nodes) {
            if (node.capabilities() != null) {
                capable.addAll(List.of(ctx.json().read(node.capabilities(), String[].class)));
            }
        }
        List<String> warnings = new ArrayList<>();
        for (JobDefinition job : ctx.catalog().all()) {
            if (job.enabled() && !capable.contains(job.handlerClass())) {
                warnings.add("no node of color " + color + " can run job " + job.name() + " (" + job.handlerClass()
                        + "), its runs wait");
            }
        }
        return warnings;
    }
}
