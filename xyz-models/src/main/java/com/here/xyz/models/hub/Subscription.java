/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.models.hub;

public class Subscription {

    /**
     * The unique identifier of the subscription
      */
    private String id;

    /**
     * The source of the subscribed notification (usually the space)
     */
    private String source;

    /**
     * The destination of the subscribe notification
     */
    private String destination;

    /**
     * The type of the destination
     */
    private String destinationType;

    /**
     * The configuration of the subscription
     */
    private SubscriptionConfig config;

    private SubscriptionStatus status;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Subscription withId(String id) {
        this.id = id;
        return this;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Subscription withSource(String source) {
        this.source = source;
        return this;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public Subscription withDestination(String destination) {
        this.destination = destination;
        return this;
    }

    public String getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(String destinationType) {
        this.destinationType = destinationType;
    }

    public Subscription withDestinationType(String destinationType) {
        this.destinationType = destinationType;
        return this;
    }

    public SubscriptionConfig getConfig() {
        return config;
    }

    public void setConfig(SubscriptionConfig config) {
        this.config = config;
    }

    public Subscription withConfig(SubscriptionConfig config) {
        this.config = config;
        return this;
    }

    public SubscriptionStatus getStatus() {
        return status;
    }

    public void setStatus(SubscriptionStatus status) {
        this.status = status;
    }

    public Subscription withStatus(SubscriptionStatus status) {
        this.status = status;
        return this;
    }

    public static class SubscriptionConfig {

        /**
         * The type of the subscription
         */
        private SubscriptionType type;

        public SubscriptionType getType() {
            return type;
        }

        public void setType(SubscriptionType type) {
            this.type = type;
        }

        public SubscriptionConfig withType(SubscriptionType type) {
            this.type = type;
            return this;
        }

        public enum SubscriptionType {
            PER_FEATURE, PER_TRANSACTION
        }
    }

    public static class SubscriptionStatus {

        /**
         * The type of the subscription
         */
        private State state;

        private String reason;

        public State getState() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }

        public SubscriptionStatus withState(State state) {
            this.state = state;
            return this;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }

        public SubscriptionStatus withReason(String reason) {
            this.reason = reason;
            return this;
        }

        public enum State {
            ACTIVE, INACTIVE, SUSPENDED, PENDING, AUTH_FAILED
        }
    }
}


