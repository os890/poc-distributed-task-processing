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
package org.os890.dtp.core.domain;

import java.util.UUID;

import org.os890.dtp.core.DtpException;

/**
 * Thrown by the fence check if the run no longer holds the lease of its job (DD-12). The current unit of work is
 * rolled back, nothing of it becomes visible. The new owner of the lease redoes the work.
 */
public class LeaseLostException extends DtpException {

    private static final long serialVersionUID = 1L;

    public LeaseLostException(UUID runId, long fencingToken) {
        super("run " + runId + " lost the lease of its job (fencing token " + fencingToken + ")");
    }
}
