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

package com.here.xyz.hub.rest.admin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.here.xyz.hub.Service;

@JsonTypeInfo(use = Id.CLASS)
public abstract class AdminMessage {

  /**
   * Whether the action to be executed should also be executed at the source node.
   */
  @JsonIgnore //As this information is only necessary for the local node it's not serialized
  protected boolean broadcastIncludeLocalNode = false;

  /**
   * The node which sent the message.
   */
  public final Node source = Node.OWN_INSTANCE;

  /**
   * If defined this message is only relevant for the given node.
   * If not defined the message is relevant for all nodes except the source node.
   */
  public Node destination;

  /**
   * The actions to be done when this method is received by a {@link #destination} node.
   * This method will be called automatically upon receiving the message.
   */
  protected abstract void handle();

  /**
   * Sends this message to the {@link #destination} or (if not defined) to all other nodes.
   */
  public void send() {
    Service.messageBroker.sendMessage(this);
  }

  /**
   * Sends this message to the specified {@link #destination} node.
   * Calling this method is the same as calling {@link #send()} while {@link #destination} is set to some node.
   * @param destination
   */
  public void send(Node destination) {
    this.destination = destination;
    send();
  }

  /**
   * Sends this message to all other nodes.
   * Calling this method is the same as calling {@link #send()} while {@link #destination} is set to null.
   */
  public void broadcast() {
    this.destination = null;
    send();
  }

  public AdminMessage withBroadcastIncludeLocalNode(boolean broadcastIncludeLocalNode) {
    this.broadcastIncludeLocalNode = broadcastIncludeLocalNode;
    return this;
  }

  public AdminMessage withDestination(Node destination) {
    this.destination = destination;
    return this;
  }
}
