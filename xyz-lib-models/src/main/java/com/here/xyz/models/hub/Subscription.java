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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.implementation.Feature;
import org.jetbrains.annotations.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Subscription")
public final class Subscription extends Feature {

  @JsonCreator
  public Subscription(@JsonProperty @Nullable String id) {
    super(id);
  }

  /**
   * The source of the subscribed notification (usually the space).
   */
  @JsonProperty
  @JsonView(All.class)
  private String source;

  /**
   * The destination of the subscribe notification.
   */
  @JsonProperty
  @JsonView(All.class)
  private String destination;

  /**
   * The configuration of the subscription.
   */
  @JsonProperty
  @JsonView(All.class)
  private SubscriptionConfig config;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private SubscriptionStatus status;

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

}