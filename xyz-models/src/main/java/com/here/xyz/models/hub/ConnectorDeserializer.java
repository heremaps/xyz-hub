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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConnectorDeserializer extends StdDeserializer<Map<String, List<Space.ListenerConnectorRef>>> {
  @SuppressWarnings("unused")
  public ConnectorDeserializer() {
    this(null);
  }

  private ConnectorDeserializer(Class<?> vc) {
    super(vc);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, List<Space.ListenerConnectorRef>> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    // check if token is ARRAY_START or OBJECT_START
    JsonToken token = jsonParser.currentToken();
    if (token == JsonToken.START_ARRAY) {
      // Old style array of Listeners, convert to Map
      List<Space.ListenerConnectorRef> listeners = jsonParser.readValueAs(new TypeReference<List<Space.ListenerConnectorRef>>() {});
      Map<String, List<Space.ListenerConnectorRef>> result = new TreeMap<>();
      int i = 0;
      for (Space.ListenerConnectorRef l : listeners) {
        result.computeIfAbsent(l.getId(), k -> new LinkedList<>()).add((Space.ListenerConnectorRef) l.withOrder(i++).withId(null));
      }
      return result;
    } else if (token == JsonToken.START_OBJECT) {
      // already new format, just deserialize and return
      return jsonParser.readValueAs(new TypeReference<Map<String, List<Space.ListenerConnectorRef>>>() {
      });
    } else {
      throw new IOException("Current token is neither START_ARRAY nor START_OBJECT, instead " + token);
    }
  }
}
