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
package org.os890.dtp.rest;

import java.util.function.Supplier;

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;

import org.os890.dtp.core.DtpException;

/** Maps the exceptions of the engine to HTTP: unknown things 404, refused operations 409, missing input 400. */
class DtpErrors {

    private DtpErrors() {
    }

    static <T> T call(Supplier<T> action) {
        try {
            return action.get();
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("parameters of job")) {
                throw new BadRequestException(e.getMessage(), e);
            }
            throw new NotFoundException(e.getMessage(), e);
        } catch (DtpException | IllegalStateException e) {
            throw new ClientErrorException(e.getMessage(), Response.Status.CONFLICT, e);
        } catch (NullPointerException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
    }

    static void run(Runnable action) {
        call(() -> {
            action.run();
            return null;
        });
    }
}
