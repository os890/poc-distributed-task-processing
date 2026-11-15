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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.SecurityContext;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.Decider;

/**
 * Who calls the REST API, and what the caller may do (mission D22). Two modes, {@code dtp.security.mode}:
 * <ul>
 * <li>{@code jwt} (default): the user is the principal of the {@link SecurityContext}, its roles and teams are checked
 * with {@link SecurityContext#isUserInRole(String)}, so MP JWT {@code groups} work on any MicroProfile runtime.</li>
 * <li>{@code off}, for development and tests only: no role checks; the user and the teams come from the principal if
 * there is one, else from the headers {@code X-User} and {@code X-Groups} (comma-separated).</li>
 * </ul>
 * {@code dtp-admin} implies every other role.
 */
@ApplicationScoped
public class DtpAccess {

    /** Reads the cluster, runs, items, journeys and the monitoring. */
    public static final String VIEWER = "dtp-viewer";

    /** Operates runs and packages, sees technical details, decides technical failures. */
    public static final String OPERATOR = Decisions.OPERATORS;

    /** Decides business decision requests of the teams it is a member of. */
    public static final String BUSINESS = "dtp-business";

    /** Changes definitions and flows; implies all other roles. */
    public static final String ADMIN = "dtp-admin";

    static final String USER_HEADER = "X-User";
    static final String GROUPS_HEADER = "X-Groups";

    @Inject
    @ConfigProperty(name = "dtp.security.mode", defaultValue = "jwt")
    String mode;

    /**
     * The caller of a request.
     *
     * @param security the security context of the request
     * @param headers  the headers of the request
     * @return the caller
     */
    public Caller caller(SecurityContext security, HttpHeaders headers) {
        boolean off = "off".equals(mode == null ? "jwt" : mode.trim().toLowerCase(Locale.ROOT));
        Principal principal = security == null ? null : security.getUserPrincipal();
        String user = principal != null ? principal.getName() : off ? header(headers, USER_HEADER) : null;
        Set<String> groups = off ? groups(header(headers, GROUPS_HEADER)) : Set.of();
        return new Caller(off, user == null || user.isBlank() ? null : user, security, groups);
    }

    private static String header(HttpHeaders headers, String name) {
        return headers == null ? null : headers.getHeaderString(name);
    }

    private static Set<String> groups(String header) {
        return header == null ? Set.of() : Arrays.stream(header.split(",")).map(String::trim)
                .filter(g -> !g.isEmpty()).collect(Collectors.toSet());
    }

    /** The caller of a request: its user, roles and teams. */
    public static class Caller {

        private final boolean off;
        private final String user;
        private final SecurityContext security;
        private final Set<String> headerGroups;

        Caller(boolean off, String user, SecurityContext security, Set<String> headerGroups) {
            this.off = off;
            this.user = user;
            this.security = security;
            this.headerGroups = headerGroups;
        }

        /**
         * Requires one of the roles ({@code dtp-admin} always passes); nothing in mode {@code off}.
         *
         * @param roles the roles
         * @return this caller
         * @throws NotAuthorizedException without a user
         * @throws ForbiddenException     without one of the roles
         */
        public Caller require(String... roles) {
            if (off) {
                return this;
            }
            if (user == null) {
                throw new NotAuthorizedException("authentication required", "Bearer");
            }
            if (Arrays.stream(roles).noneMatch(this::isInRole)) {
                throw new ForbiddenException("one of the roles " + List.of(roles) + " is required");
            }
            return this;
        }

        /**
         * Whether the caller has a role or is a member of a team ({@code dtp-admin} has every role).
         *
         * @param role the role or team
         * @return {@code true} if it has it
         */
        public boolean isInRole(String role) {
            if (off) {
                return headerGroups.contains(role) || headerGroups.contains(ADMIN);
            }
            return security != null && (security.isUserInRole(role) || security.isUserInRole(ADMIN));
        }

        /**
         * The user.
         *
         * @return the user name, if known
         */
        public Optional<String> user() {
            return Optional.ofNullable(user);
        }

        /**
         * The user, required (e.g. for audited operations).
         *
         * @return the user name
         * @throws NotAuthorizedException without a user
         */
        public String requireUser() {
            if (user == null) {
                throw new NotAuthorizedException(off ? "X-User missing" : "authentication required", "Bearer");
            }
            return user;
        }

        /**
         * The caller as a decider: its teams among the given ones (the groups of a JWT can't be listed through the
         * security context, so they are checked one by one), and {@code dtp-operator}.
         *
         * @param teams the responsible teams in question
         * @return the decider
         */
        public Decider decider(Collection<String> teams) {
            Set<String> groups = new HashSet<>();
            if (off) {
                groups.addAll(headerGroups);
            } else {
                teams.stream().filter(t -> t != null && isInRole(t)).forEach(groups::add);
                if (isInRole(OPERATOR)) {
                    groups.add(OPERATOR);
                }
            }
            return new Decider(requireUser(), groups);
        }
    }
}
