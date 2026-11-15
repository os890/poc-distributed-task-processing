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

import java.security.Principal;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.SecurityContext;

/** Security contexts and headers of fake requests. */
class Requests {

    private Requests() {
    }

    /** A user with roles (e.g. the groups of a JWT). */
    static SecurityContext user(String name, String... roles) {
        Set<String> granted = Set.of(roles);
        return new SecurityContext() {
            @Override
            public Principal getUserPrincipal() {
                return () -> name;
            }

            @Override
            public boolean isUserInRole(String role) {
                return granted.contains(role);
            }

            @Override
            public boolean isSecure() {
                return true;
            }

            @Override
            public String getAuthenticationScheme() {
                return "Bearer";
            }
        };
    }

    /** Nobody logged in. */
    static SecurityContext anonymous() {
        return new SecurityContext() {
            @Override
            public Principal getUserPrincipal() {
                return null;
            }

            @Override
            public boolean isUserInRole(String role) {
                return false;
            }

            @Override
            public boolean isSecure() {
                return false;
            }

            @Override
            public String getAuthenticationScheme() {
                return null;
            }
        };
    }

    /** Headers of a request. */
    static HttpHeaders headers(Map<String, String> values) {
        MultivaluedMap<String, String> map = new MultivaluedHashMap<>();
        values.forEach(map::add);
        return new HttpHeaders() {
            @Override
            public List<String> getRequestHeader(String name) {
                return map.get(name);
            }

            @Override
            public String getHeaderString(String name) {
                return map.getFirst(name);
            }

            @Override
            public MultivaluedMap<String, String> getRequestHeaders() {
                return map;
            }

            @Override
            public List<MediaType> getAcceptableMediaTypes() {
                return List.of();
            }

            @Override
            public List<Locale> getAcceptableLanguages() {
                return List.of();
            }

            @Override
            public MediaType getMediaType() {
                return null;
            }

            @Override
            public Locale getLanguage() {
                return null;
            }

            @Override
            public Map<String, Cookie> getCookies() {
                return Map.of();
            }

            @Override
            public Date getDate() {
                return null;
            }

            @Override
            public int getLength() {
                return 0;
            }
        };
    }

    static HttpHeaders none() {
        return headers(Map.of());
    }
}
