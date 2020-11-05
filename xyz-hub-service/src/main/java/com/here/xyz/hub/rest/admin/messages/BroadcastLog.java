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

import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import com.here.xyz.hub.rest.admin.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A message which can be used to write a log on all nodes at (nearly) the same time.
 */
public class BroadcastLog extends RelayedMessage {

  private static final Logger logger = LogManager.getLogger();

  public final Node destination = null;
  public String logMessage;

  private BroadcastLog() {
    broadcastIncludeLocalNode = true;
  }

  public BroadcastLog(String logMessage) {
    this();
    this.logMessage = logMessage;
  }

  @Override
  protected void handleAtDestination() {
    logger.info("[BROADCAST from " + source.id + " (" + source.ip + ")] " + logMessage);
  }
}
