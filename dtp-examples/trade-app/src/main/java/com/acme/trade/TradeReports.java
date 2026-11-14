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
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * The simulated archive of the regulatory trade reports of this node (an in-memory list instead of a file share).
 */
@ApplicationScoped
public class TradeReports {

    private final List<String> files = new CopyOnWriteArrayList<>();

    /**
     * Archives a report file.
     *
     * @param name   the deterministic file name: written again after a takeover, it replaces the earlier one
     * @param trades the trade numbers in the report
     */
    void write(String name, List<String> trades) {
        files.removeIf(f -> f.startsWith(name + " "));
        files.add(name + " (" + trades.size() + " trades)");
    }

    /**
     * The report files of this node.
     *
     * @return the files, oldest first
     */
    public List<String> files() {
        return List.copyOf(files);
    }
}
