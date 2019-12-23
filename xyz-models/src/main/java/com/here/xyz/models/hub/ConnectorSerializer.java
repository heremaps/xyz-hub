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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.*;

public class ConnectorSerializer extends StdSerializer<Map<String, List<Space.ListenerConnectorRef>>> {

  private static Comparator<Space.ListenerConnectorRef> listenerComparator = Comparator.comparingInt(Space.ListenerConnectorRef::getOrder);

  @SuppressWarnings("unused")
  public ConnectorSerializer() {
    this(null);
  }

  private ConnectorSerializer(Class<Map<String, List<Space.ListenerConnectorRef>>> vc) {
    super(vc);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void serialize(Map<String, List<Space.ListenerConnectorRef>> value, JsonGenerator gen, SerializerProvider provider) throws IOException {
    if (value == null) {
      return;
    }

    // might be a bit big, but just in case
    final List<Space.ListenerConnectorRef> result = new ArrayList<>(value.size() * 2);

    // clone the entries, update them and add to another list
    for (Map.Entry<String, List<Space.ListenerConnectorRef>> entry : value.entrySet()) {
      if (entry.getValue() != null) {
        entry.getValue().forEach(l -> {
          Space.ListenerConnectorRef shallowCopy = (Space.ListenerConnectorRef) new Space.ListenerConnectorRef()
                  // only temporary for sorting
                  .withOrder(l.getOrder())
                  // following field do not change, reuse them
                  .withEventTypes(l.getEventTypes())
                  .withParams(l.getParams())
                  // our changes
                  .withId(entry.getKey());
          result.add(shallowCopy);
        });
      }
    }

    // sort the listeners/processors by order
    result.sort(listenerComparator);
    // remove the order - I do not see any way how to avoid iterating over all objects again - we need the order for
    // sorting
    result.forEach(l -> l.setOrder(null));

    gen.writeObject(result);
  }
}
