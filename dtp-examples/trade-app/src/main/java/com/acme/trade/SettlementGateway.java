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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Simulated settlement gateway: sends a file of settlement instructions to a custodian. A custodian can be
 * unavailable (demo and tests), then it rejects the file.
 */
@ApplicationScoped
public class SettlementGateway {

    /** The custodians of the example. */
    static final List<String> CUSTODIANS = List.of("OEKB-CSD", "CLEARSTREAM", "EUROCLEAR");

    private final Set<String> unavailable = ConcurrentHashMap.newKeySet();
    private final List<String> files = new CopyOnWriteArrayList<>();

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
     * Sends a file.
     *
     * @param custodian    the receiver
     * @param sequence     the number of the work package
     * @param instructions the trade numbers
     * @return the file name
     * @throws IllegalStateException if the custodian is unavailable
     */
    String send(String custodian, long sequence, List<String> instructions) {
        if (unavailable.contains(custodian)) {
            throw new IllegalStateException(custodian + " is unavailable");
        }
        String file = "settlement-" + custodian + "-" + sequence + ".xml";
        files.add(file + " (" + instructions.size() + " instructions)");
        return file;
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

    /** The files sent by this node. */
    public List<String> files() {
        return List.copyOf(files);
    }
}
