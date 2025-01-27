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
package com.here.naksha.lib.core.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.naksha.lib.core.models.geojson.implementation.LazyParsedFeatureCollectionTest;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileEvent;
import com.here.naksha.lib.core.models.payload.events.feature.IterateFeaturesEvent;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.core.view.ViewDeserialize.User;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

public class EventTest {

  private String eventJson = "{\"type\":\"IterateFeaturesEvent\",\"space\":\"my-space\",\"params\":{},\"limit\":5}";

  @Test
  public void fromJson() throws Exception {
    final InputStream is = new ByteArrayInputStream(eventJson.getBytes());
    final Event event = new ObjectMapper().readValue(is, Event.class);
    assertNotNull(event);
    assertTrue(event instanceof IterateFeaturesEvent);
  }

  @Test
  public void testClone() throws Exception {
    try (final Json json = Json.get()) {
      final Event event = json.reader(User.class).readValue(eventJson, Event.class);
      final Event clone = JsonSerializable.deepClone(event);

      assertNotSame(event, clone);
      assertTrue(event instanceof IterateFeaturesEvent);
      assertTrue(clone instanceof IterateFeaturesEvent);
      assertEquals(event.getSpaceId(), clone.getSpaceId());
      assertEquals(event.getParams(), clone.getParams());
      assertEquals(((IterateFeaturesEvent) event).getLimit(), ((IterateFeaturesEvent) clone).getLimit());
    }
  }

  @Test
  public void testDeepCopy() throws Exception {
    try (final Json json = Json.get()) {
      final Event event = json.reader(User.class).readValue(eventJson, Event.class);
      final Event clone = JsonSerializable.deepClone(event);

      assertNotSame(event, clone);
      assertTrue(event instanceof IterateFeaturesEvent);
      assertTrue(clone instanceof IterateFeaturesEvent);
      assertEquals(event.getSpaceId(), clone.getSpaceId());
      assertEquals(event.getParams(), clone.getParams());
      assertEquals(((IterateFeaturesEvent) event).getLimit(), ((IterateFeaturesEvent) clone).getLimit());
    }
  }

  @Test
  public void getFeaturesByTileEventTest() throws Exception {
    final InputStream is = LazyParsedFeatureCollectionTest.class.getResourceAsStream(
        "/com/here/xyz/test/GetFeaturesByTileEvent.json");

    GetFeaturesByTileEvent event = JsonSerializable.deserialize(is);
    assertNotNull(event.getBbox());
    assertEquals(-12.05543709446954D, event.getBbox().getNorth(), 0);
    assertEquals(-77.0745849609375D, event.getBbox().getEast(), 0);
    assertEquals(-12.060809058367298D, event.getBbox().getSouth(), 0);
    assertEquals(-77.080078125D, event.getBbox().getWest(), 0);
  }

}
