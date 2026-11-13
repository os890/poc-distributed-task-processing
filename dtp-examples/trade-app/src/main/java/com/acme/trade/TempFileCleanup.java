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

import jakarta.enterprise.context.ApplicationScoped;

import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunInfoKey;

/** Level 1 of the sample: a handler and one definition row, runs on exactly one node per cron slot. */
@ApplicationScoped
public class TempFileCleanup implements JobHandler<NoParams, DefaultOutcome> {

    public static final RunInfoKey<Long> DELETED = RunInfoKey.of("deletedFiles", Long.class);

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        ctx.runInfo().put(DELETED, 0L);
        return JobResult.success();
    }
}
