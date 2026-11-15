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

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/**
 * The REST API of dtp with MP JWT. In tests, Quarkus generates an in-memory key pair (no key in the repository): its
 * public key verifies the tokens, the test signs them with its private key ({@code Jwt...sign()}).
 */
public class JwtProfile implements QuarkusTestProfile {

    static final String ISSUER = "https://trade-app.example/issuer";

    @Override
    public Map<String, String> getConfigOverrides() {
        // its own in-memory database: the other test class counts the trades of its own imports
        return Map.of("dtp.security.mode", "jwt",
                "quarkus.datasource.jdbc.url", "jdbc:h2:mem:trade-jwt;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000");
    }
}
