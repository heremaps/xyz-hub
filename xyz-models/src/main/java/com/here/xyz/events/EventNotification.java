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

package com.here.xyz.events;

import com.here.xyz.Payload;

public class EventNotification extends Event<EventNotification> {

  private String eventType;
  private Payload event;

  public String getEventType() {
    return this.eventType;
  }

  @SuppressWarnings("unused")
  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  @SuppressWarnings("unused")
  public EventNotification withEventType(String eventType) {
    setEventType(eventType);
    return this;
  }

  public Payload getEvent() {
    return this.event;
  }

  @SuppressWarnings("unused")
  public void setEvent(Payload event) {
    this.event = event;
  }

  @SuppressWarnings("unused")
  public EventNotification withEvent(Payload event) {
    setEvent(event);
    return this;
  }
}
