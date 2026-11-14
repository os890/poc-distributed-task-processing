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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Version;

import org.os890.dtp.api.ItemType;

/** A trade of the import feed. Declares its item type once (DD-20). */
@Entity
public class Trade {

    public static final ItemType<Trade, UUID> ITEM = ItemType.of("trade", Trade.class, UUID.class);

    @Id
    private UUID id;

    @Version
    private long version;

    private String tradeNumber;

    private String isin;

    private LocalDate businessDate;

    private BigDecimal price;

    private long quantity;

    protected Trade() {
    }

    public Trade(String tradeNumber, String isin, LocalDate businessDate) {
        this.id = UUID.randomUUID();
        this.tradeNumber = tradeNumber;
        this.isin = isin;
        this.businessDate = businessDate;
    }

    public UUID getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public String getTradeNumber() {
        return tradeNumber;
    }

    public String getIsin() {
        return isin;
    }

    public void setIsin(String isin) {
        this.isin = isin;
    }

    public LocalDate getBusinessDate() {
        return businessDate;
    }

    public long getQuantity() {
        return quantity;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }
}
