/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.task;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.ModifyBranchEvent;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import io.vertx.core.Future;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PhysicalTableNameEventPropagationTest {

  private static final String PHYSICAL_TABLE_NAME = "0123456789abcdef0123456789abcdef";

  @Test
  void branchEventsReceiveThePersistedPhysicalTableName() {
    ModifyBranchEvent event = new ModifyBranchEvent().withSpace("logical-space-id");

    BranchHandler.injectStorageParams(event, spaceWithPhysicalTableName());

    assertEquals(PHYSICAL_TABLE_NAME, event.getParams().get(TABLE_NAME));
  }

  @Test
  void changesetEventsReceiveThePersistedPhysicalTableName() {
    IterateChangesetsEvent event = new IterateChangesetsEvent().withSpace("logical-space-id");

    Future<Void> injection = SpaceConnectorBasedHandler.injectStorageParams(null, event, spaceWithPhysicalTableName());

    assertTrue(injection.succeeded());
    assertEquals(PHYSICAL_TABLE_NAME, event.getParams().get(TABLE_NAME));
  }

  private static Space spaceWithPhysicalTableName() {
    Space space = new Space();
    space.setId("logical-space-id");
    space.setStorage(new ConnectorRef()
        .withId("psql")
        .withParams(Map.of(TABLE_NAME, PHYSICAL_TABLE_NAME)));
    return space;
  }
}
