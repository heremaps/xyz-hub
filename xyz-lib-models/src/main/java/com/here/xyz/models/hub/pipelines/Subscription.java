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

package com.here.xyz.models.hub.pipelines;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.INaksha;
import com.here.xyz.models.hub.plugins.Storage;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The subscription configuration. Used to create and execute pipelines, when transaction events
 * happen to storages.
 */
@SuppressWarnings("unused")
@JsonTypeName(value = "Subscription")
@AvailableSince(INaksha.v2_0)
public final class Subscription extends PipelineComponent {

    @AvailableSince(INaksha.v2_0)
    public static final String STORAGE_ID = "storageId";

    /**
     * Create a new subscription.
     *
     * @param id the identifier.
     * @param eventHandlers the list of event handler identifiers to form the event-pipeline.
     * @param storageId the identifier of the storage to observe.
     */
    @JsonCreator
    @AvailableSince(INaksha.v2_0)
    public Subscription(
            @JsonProperty(ID) @NotNull String id,
            @JsonProperty(EVENT_HANDLERS) @NotNull List<@NotNull String> eventHandlers,
            @JsonProperty(STORAGE_ID) @NotNull String storageId) {
        super(id, eventHandlers, null);
        this.storageId = storageId;
    }

    /**
     * Create a new subscription.
     *
     * @param id the identifier.
     * @param eventHandlers the list of event handler identifiers to form the event-pipeline.
     * @param storageId the identifier of the storage to observe.
     * @param packages the packages this feature is part of.
     */
    @JsonCreator
    @AvailableSince(INaksha.v2_0)
    public Subscription(
            @JsonProperty(ID) @NotNull String id,
            @JsonProperty(EVENT_HANDLERS) @NotNull List<@NotNull String> eventHandlers,
            @JsonProperty(STORAGE_ID) @NotNull String storageId,
            @Nullable List<@NotNull String> packages) {
        super(id, eventHandlers, packages);
        this.storageId = storageId;
    }

    /** The {@link Storage#id identifier of the storage} to observe. */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(STORAGE_ID)
    public @NotNull String storageId;

    /** The destination of the subscribe notification. */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty
    private @Nullable String destination;

    /** The configuration of the subscription. */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty
    private @Nullable SubscriptionConfig config;

    @AvailableSince(INaksha.v2_0)
    @JsonProperty
    @JsonInclude(Include.NON_NULL)
    private @Nullable SubscriptionStatus status;

    public @Nullable String getDestination() {
        return destination;
    }

    public void setDestination(@Nullable String destination) {
        this.destination = destination;
    }

    public @NotNull Subscription withDestination(@Nullable String destination) {
        this.destination = destination;
        return this;
    }

    public @Nullable SubscriptionConfig getConfig() {
        return config;
    }

    public void setConfig(@Nullable SubscriptionConfig config) {
        this.config = config;
    }

    public @NotNull Subscription withConfig(@Nullable SubscriptionConfig config) {
        this.config = config;
        return this;
    }

    public @Nullable SubscriptionStatus getStatus() {
        return status;
    }

    public void setStatus(@Nullable SubscriptionStatus status) {
        this.status = status;
    }

    public @NotNull Subscription withStatus(@Nullable SubscriptionStatus status) {
        this.status = status;
        return this;
    }
}
