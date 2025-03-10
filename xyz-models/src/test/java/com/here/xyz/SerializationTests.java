/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable.Public;
import java.util.Map;

import com.here.xyz.models.geojson.implementation.Feature;
import org.junit.Test;

import static org.junit.Assert.*;

public class SerializationTests {

  @Test
  public void testPublicSerialization() throws JsonProcessingException {
    TestModel object = new TestModel();
    final String serialized = object.serialize(Public.class, true);
    Map<String, Object> jsonMap = XyzSerializable.deserialize(serialized, Map.class);

    assertFalse(jsonMap.containsKey("somePrivateIntWithDefaultValue"));
    assertFalse(jsonMap.containsKey("somePrivateZeroInt"));
    assertFalse(jsonMap.containsKey("somePrivateIntWithInternalView"));
    assertFalse(jsonMap.containsKey("somePrivateIntWithStaticView"));

    assertTrue(jsonMap.containsKey("somePrivateIntWithPublicView"));
    assertTrue(jsonMap.containsKey("somePublicIntWithDefaultValue"));
    assertTrue(jsonMap.containsKey("someIntValue"));
  }

  @Test
  public void testDeprecatedDefaultSerialization() throws JsonProcessingException {
    TestModel object = new TestModel();
    final String serialized = object.serialize(null, true);
    Map<String, Object> jsonMap = XyzSerializable.deserialize(serialized, Map.class);

    assertFalse(jsonMap.containsKey("somePrivateIntWithDefaultValue"));
    assertFalse(jsonMap.containsKey("somePrivateZeroInt"));

    assertTrue(jsonMap.containsKey("somePrivateIntWithInternalView"));
    assertTrue(jsonMap.containsKey("somePrivateIntWithStaticView"));
    assertTrue(jsonMap.containsKey("somePrivateIntWithPublicView"));
    assertTrue(jsonMap.containsKey("somePublicIntWithDefaultValue"));
    assertTrue(jsonMap.containsKey("someIntValue"));
  }

  @Test
  public void testStreamIterator() throws Exception {
    var it = XyzSerializable.deserializeJsonLines(
            JsonMappingTest.class.getResourceAsStream("test/features_array.jsonl"), Feature.class);
    while (it.hasNext()) {
      Feature feature = it.next();
      assertNotNull(feature);
      assertTrue(feature instanceof Feature);
    }
  }



  private static class TestModel implements Typed {

    private int somePrivateIntWithDefaultValue = 23;
    private int somePrivateZeroInt;
    @JsonView(Internal.class)
    private int somePrivateIntWithInternalView = 27;
    @JsonView(Static.class)
    private int somePrivateIntWithStaticView = 27;
    @JsonView(Public.class)
    private int somePrivateIntWithPublicView = 27;

    public int somePublicIntWithDefaultValue = 42;

    public int getSomeIntValue() {
      return somePrivateIntWithDefaultValue;
    }

    public void setSomeIntValue(int someIntValue) {
      somePrivateIntWithDefaultValue = someIntValue;
    }
  }
}
