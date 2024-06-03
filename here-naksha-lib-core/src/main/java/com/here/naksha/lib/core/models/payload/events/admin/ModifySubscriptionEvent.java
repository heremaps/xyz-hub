/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.lib.core.models.payload.events.admin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.features.Subscription;
import com.here.naksha.lib.core.models.payload.Event;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ModifySubscriptionEvent")
public class ModifySubscriptionEvent extends Event {

  private Operation operation;
  private Subscription subscription;
  private boolean hasNoActiveSubscriptions;

  @SuppressWarnings("unused")
  public Operation getOperation() {
    return this.operation;
  }

  @SuppressWarnings("WeakerAccess")
  public void setOperation(Operation operation) {
    this.operation = operation;
  }

  @SuppressWarnings("unused")
  public ModifySubscriptionEvent withOperation(Operation operation) {
    setOperation(operation);
    return this;
  }

  @SuppressWarnings("unused")
  public Subscription getSubscription() {
    return this.subscription;
  }

  public void setSubscription(Subscription subscription) {
    this.subscription = subscription;
  }

  @SuppressWarnings("unused")
  public ModifySubscriptionEvent withSubscription(Subscription subscription) {
    setSubscription(subscription);
    return this;
  }

  public boolean getHasNoActiveSubscriptions() {
    return hasNoActiveSubscriptions;
  }

  public void setHasNoActiveSubscriptions(boolean hasNoActiveSubscriptions) {
    this.hasNoActiveSubscriptions = hasNoActiveSubscriptions;
  }

  public ModifySubscriptionEvent withHasNoActiveSubscriptions(boolean hasNoActiveSubscriptions) {
    this.hasNoActiveSubscriptions = hasNoActiveSubscriptions;
    return this;
  }

  public enum Operation {
    CREATE,
    UPDATE,
    DELETE
  }
}
