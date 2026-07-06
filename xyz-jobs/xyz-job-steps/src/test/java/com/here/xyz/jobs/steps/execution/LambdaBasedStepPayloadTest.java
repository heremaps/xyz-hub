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

package com.here.xyz.jobs.steps.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

public class LambdaBasedStepPayloadTest {

  @Test
  void compactStateCheckInputRemovesLargeFilterFields() {
    String input = """
        {
          "type":"STATE_CHECK",
          "step":{
            "id":"s_bahygt",
            "jobId":"okjcygbdjn",
            "taskToken":"token",
            "spatialFilter":{"geometry":{"type":"MultiPolygon","coordinates":[[[[1,2],[3,4],[1,2]]]]}},
            "propertyFilter":[[{"key":"foo","operation":"EQUALS","values":["bar"]}]],
            "nested":{"spatialFilter":{"geometry":{"type":"Polygon"}},"keep":true}
          }
        }
        """;

    String compacted = LambdaBasedStep.compactStateCheckInput(input);
    JsonObject result = new JsonObject(compacted);
    JsonObject step = result.getJsonObject("step");
    assertEquals("s_bahygt", step.getString("id"));
    assertEquals("okjcygbdjn", step.getString("jobId"));
    assertEquals("token", step.getString("taskToken"));

    assertFalse(step.containsKey("spatialFilter"));

    JsonObject nested = step.getJsonObject("nested");
    assertTrue(nested.getBoolean("keep"));
    assertFalse(nested.containsKey("spatialFilter"));
  }
}

