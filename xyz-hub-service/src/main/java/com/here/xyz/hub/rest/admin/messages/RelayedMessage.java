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

package com.here.xyz.hub.rest.admin.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.xyz.hub.rest.admin.AdminMessage;
import com.here.xyz.hub.rest.admin.Node;

public abstract class RelayedMessage extends AdminMessage {

  public boolean relay = false;
  public Node relayedBy;
  /**
   * Whether the this message was relayed by the local node.
   * If that's the case it will be handled directly locally and the following broad-casted message coming
   * through the network will be refused.
   */
  @JsonIgnore //As this information is only necessary for the local node it's not serialized
  private boolean relayedLocally = false;

  public RelayedMessage withRelay(boolean relay) {
    this.relay = relay;
    return this;
  }

  /**
   * This method will be called at the relay and at the final {@link #destination} node. It will only care about the relaying.
   * If applicable it will delegate to {@link #handleAtDestination()} at the {@link #destination} node which contains the actual
   * implementation about the actions to be done.
   */
  @Override
  protected final void handle() {
    if (relayedLocally) {
      //Reset relayedBy here as actually this message was not relayed. It get's handled directly in place.
      relayedBy = null;
    }
    if (relay) {
      relay = false;
      if (!Node.OWN_INSTANCE.equals(destination)) {
        relayedBy = Node.OWN_INSTANCE;
        relayedLocally = true;
        send();
      }
      else
        handleAtDestination();
    }
    else if (!Node.OWN_INSTANCE.equals(relayedBy))
      handleAtDestination();
  }

  /**
   * The actions to be done when this method is received by a {@link #destination} node.
   * This method will be called automatically upon receiving the message.
   */
  protected abstract void handleAtDestination();

}
