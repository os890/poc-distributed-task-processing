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

import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.os890.dtp.api.Approval;
import org.os890.dtp.api.ApprovalResult;
import org.os890.dtp.api.BranchResult;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.TrackedItem;

/**
 * The join step of the parallel checks: rejects restricted trades and adds market data. Four batches of 25 trades run
 * in parallel, each in its own fenced transaction. Trades above the limit need the approval of two members of the risk
 * desk (four-eyes principle); they come back to this step afterwards.
 */
@ApplicationScoped
@ItemProcessing(batchSize = 25, parallelism = 4)
public class TradeEnrichment implements ItemProcessor<Trade, UUID> {

    @Inject
    MarketData marketData;

    @Inject
    EntityManager em;

    @Override
    public void process(TrackedItem<Trade, UUID> item) {
        Optional<ApprovalResult> approval = item.approval();
        if (approval.isPresent()) {
            em.find(Trade.class, item.ref().id()).setReleased(approval.get().approved());
            item.signal(approval.get().approved() ? EnrichmentSignal.ENRICHED : EnrichmentSignal.REJECTED,
                    (approval.get().approved() ? "approved by " + approval.get().approvers()
                                               : "rejected by " + approval.get().decidedBy()));
            return;
        }
        // the results of the parallel branches
        BranchResult compliance = item.branchResult(ComplianceCheck.class).orElseThrow();
        if (compliance.is(ComplianceSignal.RESTRICTED)) {
            item.signal(EnrichmentSignal.REJECTED, compliance.reason());
            return;
        }
        Trade trade = em.find(Trade.class, item.ref().id());
        marketData.enrich(trade);              // same transaction as the routing
        BranchResult limit = item.branchResult(LimitCheck.class).orElseThrow();
        if (limit.is(LimitSignal.ABOVE_LIMIT)) {
            item.requestApproval(Approval.quorum(2).assignedTo(Desk.RISK), limit.reason());
        } else {
            trade.setReleased(true);
            item.signal(EnrichmentSignal.ENRICHED);
        }
    }
}
