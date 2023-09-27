/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * That message can be used to change the log-level of one or more service-nodes. The specified level must be a valid log-level. As this is
 * a {@link RelayedMessage} it can be sent to a specific service-node or to all service-nodes regardless of the first service node by which
 * it was received.
 *
 * Specifying the property {@link RelayedMessage#relay} to true will relay the message to the specified destination. If no destination is
 * specified the message will be relayed to all service-nodes (broadcast).
 */
@SuppressWarnings("unused")
class ChangeLogLevelMessage extends RelayedMessage {

  private static final Logger logger = LogManager.getLogger();

  private String level;

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  @Override
  protected void handleAtDestination() {
    logger.info("LOG LEVEL UPDATE requested. New level will be: " + level);
    Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.getLevel(level));
    logger.info("LOG LEVEL UPDATE performed. New level is now: " + level);
  }
}
