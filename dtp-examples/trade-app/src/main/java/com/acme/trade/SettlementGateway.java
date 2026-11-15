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
package com.acme.trade;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.os890.dtp.core.engine.Acknowledgements;
import org.os890.dtp.core.engine.Acknowledgements.Outcome;

/**
 * Simulated settlement gateway: sends a file of settlement instructions to a custodian. The custodian answers later
 * (after {@code trade.settlement.ack-delay}) with an acknowledgement, through the callback of the application
 * ({@link Acknowledgements}). For demos and tests a custodian can be unavailable (the file can't be sent), reject its
 * files, or not answer at all.
 */
@ApplicationScoped
public class SettlementGateway {

    /** The custodians of the example. */
    static final List<String> CUSTODIANS = List.of("OEKB-CSD", "CLEARSTREAM", "EUROCLEAR");

    /** How a custodian answers. */
    public enum Mode {
        /** Acknowledges every file. */
        ACCEPTING,
        /** Rejects every file. */
        REJECTING,
        /** Doesn't answer: the acknowledgement times out. */
        SILENT
    }

    /** Attempts of a callback whose file isn't known yet (the send wasn't committed yet). */
    private static final int CALLBACK_ATTEMPTS = 10;

    private final Set<String> unavailable = ConcurrentHashMap.newKeySet();
    private final Map<String, Mode> modes = new ConcurrentHashMap<>();
    private final List<String> files = new CopyOnWriteArrayList<>();
    private final List<String> answers = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService custodians = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("custodians").factory());

    @Inject
    Instance<Acknowledgements> acknowledgements;

    @ConfigProperty(name = "trade.settlement.ack-delay", defaultValue = "PT1S")
    Duration ackDelay;

    /**
     * The custodian that settles a trade (example rule: by its trade number).
     *
     * @param trade the trade
     * @return the custodian
     */
    String custodianOf(Trade trade) {
        return CUSTODIANS.get(Math.floorMod(trade.getTradeNumber().hashCode(), CUSTODIANS.size()));
    }

    /**
     * Sends a file. The custodian answers later.
     *
     * @param custodian    the receiver
     * @param sequence     the number of the work package
     * @param instructions the trade numbers
     * @return the file name, the reference of the acknowledgement
     * @throws IllegalStateException if the custodian is unavailable
     */
    String send(String custodian, long sequence, List<String> instructions) {
        if (unavailable.contains(custodian)) {
            throw new IllegalStateException(custodian + " is unavailable");
        }
        String file = "settlement-" + custodian + "-" + sequence + ".xml";
        files.add(file + " (" + instructions.size() + " instructions)");
        Mode mode = modes.getOrDefault(custodian, Mode.ACCEPTING);
        if (mode != Mode.SILENT) {
            answerLater(custodian, file, mode == Mode.ACCEPTING, 1);
        }
        return file;
    }

    /** The custodian's callback; repeated while the file isn't known yet, like a receiver retrying on 404. */
    private void answerLater(String custodian, String file, boolean accepted, int attempt) {
        custodians.schedule(() -> {
            Outcome outcome = accepted
                    ? acknowledgements.get().acknowledge(SettlementSender.class, file, custodian)
                    : acknowledgements.get().reject(SettlementSender.class, file, custodian + " rejected " + file
                            + ": unknown safekeeping account");
            if (outcome == Outcome.UNKNOWN && attempt < CALLBACK_ATTEMPTS) {
                answerLater(custodian, file, accepted, attempt + 1);
            } else {
                answers.add(file + ": " + outcome);
            }
        }, ackDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    boolean isAvailable(String custodian) {
        return !unavailable.contains(custodian);
    }

    /**
     * Makes a custodian (un)available.
     *
     * @param custodian the custodian
     * @param available {@code true} if it accepts files
     */
    public void setAvailable(String custodian, boolean available) {
        if (available) {
            unavailable.remove(custodian);
        } else {
            unavailable.add(custodian);
        }
    }

    /**
     * Sets how a custodian answers.
     *
     * @param custodian the custodian
     * @param mode      the mode
     */
    public void setMode(String custodian, Mode mode) {
        modes.put(custodian, mode);
    }

    /** The files sent by this node. */
    public List<String> files() {
        return List.copyOf(files);
    }

    /** The answers of the custodians to the files of this node, with the outcome. */
    public List<String> answers() {
        return List.copyOf(answers);
    }

    @PreDestroy
    void close() {
        custodians.shutdownNow();
    }
}
