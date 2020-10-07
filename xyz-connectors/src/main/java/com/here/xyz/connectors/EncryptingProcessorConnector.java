/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.connectors;

import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ListenerConnectorRef;
import java.util.HashSet;
import java.util.List;

/**
 * This class could be extended by any processor connector implementations that want automatic encryption of secrets.
 */
@SuppressWarnings({"WeakerAccess"})
public abstract class EncryptingProcessorConnector extends ProcessorConnector {

  /**
   * {@link HashSet} that contains all fields that should be encrypted.
   */
  protected final HashSet<String> fieldsToEncrypt = new HashSet<>();

  /**
   * My own connector ID.
   */
  protected String connectorId;

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("RedundantThrows")
  @Override
  protected ModifySpaceEvent processModifySpace(ModifySpaceEvent event, NotificationParams notificationParams) throws Exception {
    Space space = event.getSpaceDefinition();
    if (space != null)  {
      List<ListenerConnectorRef> processors = space.getProcessors().get(connectorId);
      if (processors != null) {
        processors.forEach(p -> p.setParams(eventDecryptor.encryptParams(p.getParams(), fieldsToEncrypt, space.getId())));
      }
    }
    event.setSpaceDefinition(space);
    return event;
  }
}
