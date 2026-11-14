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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Stand-in for the notification channel of the bank (mail, chat): logs the messages and keeps the latest ones for the
 * demo ({@code GET /notifications}).
 */
@ApplicationScoped
public class TeamNotifications {

    private static final System.Logger LOG = System.getLogger(TeamNotifications.class.getName());

    private static final int KEPT = 100;

    private final Deque<Message> latest = new ArrayDeque<>();

    /**
     * A sent message.
     *
     * @param audience the team
     * @param subject  the subject
     * @param body     the text
     */
    public record Message(String audience, String subject, String body) {
    }

    /**
     * Sends a message to a team.
     *
     * @param audience the team
     * @param subject  the subject
     * @param body     the text
     */
    public synchronized void send(String audience, String subject, String body) {
        LOG.log(System.Logger.Level.INFO, "[{0}] {1}", audience, subject);
        latest.addFirst(new Message(audience, subject, body));
        if (latest.size() > KEPT) {
            latest.removeLast();
        }
    }

    /**
     * The latest messages, newest first.
     *
     * @return the messages
     */
    public synchronized List<Message> latest() {
        return List.copyOf(latest);
    }
}
