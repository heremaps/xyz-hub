/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
package com.here.xyz.jobs.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Step.OutputSet;
import com.here.xyz.jobs.steps.Step.Visibility;
import org.junit.Test;

public class StepOutputSetSerializationTest {

  private static final ObjectMapper mapper = XyzSerializable.Mappers.DEFAULT_MAPPER.get();

  @Test
  public void roundTripWithSystemVisibilityAndDefaults() throws Exception {
    OutputSet original = new OutputSet("test", Visibility.SYSTEM, true)
        .withJobId("job-1")
        .withStepId("step-1");

    String json = mapper.writeValueAsString(original);
    assertNotNull(json);

    JsonNode node = mapper.readTree(json);
    assertFalse(node.has("visibility"));

    OutputSet restored = mapper.readValue(json, OutputSet.class);
    assertNotNull(restored);

    assertEquals(original, restored);
    assertEquals(original.modelBased, restored.modelBased);
  }

  @Test
  public void roundTripWithUserVisibilityAndCustomSuffix() throws Exception {
    OutputSet original = new OutputSet("test", Visibility.USER, ".json")
        .withJobId("job-2")
        .withStepId("step-2");
    original.modelBased = false;

    String json = mapper.writeValueAsString(original);
    assertNotNull(json);

    JsonNode node = mapper.readTree(json);
    assertTrue(node.has("visibility"));
    assertEquals("USER", node.get("visibility").asText());
    assertEquals(".json", node.get("fileSuffix").asText());

    OutputSet restored = mapper.readValue(json, OutputSet.class);
    assertNotNull(restored);

    assertEquals(original, restored);
    assertEquals(original.modelBased, restored.modelBased);
  }

  @Test
  public void deserializeFromExplicitJson() throws Exception {
    String json = "{" +
        "\"name\":\"explicit\"," +
        "\"visibility\":\"USER\"," +
        "\"fileSuffix\":\".json\"," +
        "\"modelBased\":true," +
        "\"jobId\":\"job-3\"," +
        "\"stepId\":\"step-3\"" +
        "}";

    OutputSet os = mapper.readValue(json, OutputSet.class);
    assertNotNull(os);
    assertEquals("explicit", os.name);
    assertEquals(".json", os.fileSuffix);
    assertTrue(os.modelBased);
    assertEquals("job-3", os.getJobId());
    assertEquals("step-3", os.getStepId());
    assertEquals(Visibility.USER, os.visibility);

    String serialized = mapper.writeValueAsString(os);
    JsonNode node = mapper.readTree(serialized);
    assertEquals("explicit", node.get("name").asText());
    assertEquals("USER", node.get("visibility").asText());
    assertEquals(".json", node.get("fileSuffix").asText());
    assertTrue(node.get("modelBased").asBoolean());
    assertEquals("job-3", node.get("jobId").asText());
    assertEquals("step-3", node.get("stepId").asText());
  }
}
