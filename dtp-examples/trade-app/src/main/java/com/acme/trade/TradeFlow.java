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

import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;

/**
 * The trade flow: the steps report signals, this definition decides where a trade goes next. At startup it is stored
 * as a flow version in the DB (a changed definition is a new version), operators can switch versions and overrule
 * single hops without a deployment. The jobs of the steps are generated, the trigger of the entry step is defined in
 * {@code db/dtp-definitions}. A valid trade is checked by two parallel branches, the enrichment joins their results.
 */
@ApplicationScoped
public class TradeFlow implements FlowDefinition {

    @Override
    public void define(FlowBuilder flow) {
        flow.entry(TradeImport.class)
                .on(ImportSignal.IMPORTED).to(TradeValidation.class)
                .on(ImportSignal.REIMPORTED).to(TradeValidation.class);
        flow.step(TradeValidation.class)
                .on(ValidationSignal.VALID).fork(LimitCheck.class).and(ComplianceCheck.class)
                    .joinAt(TradeEnrichment.class)
                .on(ValidationSignal.MISSING_ISIN).back(TradeImport.class);
        flow.step(TradeEnrichment.class)
                .on(EnrichmentSignal.ENRICHED).complete()
                .on(EnrichmentSignal.REJECTED).discard();
    }
}
