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

package com.here.xyz.models.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.STRICT;

class DataReferenceSerializationTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  private final DataReference dataReference = new DataReference()
    .withId(UUID.fromString("308a8ebd-de83-42ac-a5ce-e83bf5c603d7"))
    .withEntityId("some-entity-id")
    .withPatch(true)
    .withStartVersion(123)
    .withEndVersion(134)
    .withObjectType("some-object-type")
    .withContentType("some-content-type")
    .withContentEncoding("gzip")
    .withFilter(Map.of("jsonPath", "properties.category", "spatial", Map.of("type", "bbox")))
    .withProducer("some-producer")
    .withLocation("some-location")
    .withSourceSystem("some-source-system")
    .withTargetSystem("some-target-system");

  private final String dataReferenceJson = loadFile("data-references/data-reference.json");

  @Test
  void shouldSerializeToJson() throws Exception {
    // when
    String serialized = dataReference.serialize();

    // then
    JSONAssert.assertEquals(dataReferenceJson, serialized, STRICT);
  }

  @Test
  void shouldDeserializeFromJson() throws Exception {
    // when
    DataReference deserialized = mapper.readValue(dataReferenceJson, DataReference.class);

    // then
    assertEquals(dataReference, deserialized);
  }

  public static String loadFile(String filePathRelativeOfTestResources) {
    try {
      Path absoluteFilePath = absulteFilePath(filePathRelativeOfTestResources);
      return Files.readString(absoluteFilePath);
    } catch (IOException ex) {
      throw new RuntimeException(
        "Unable to load file for path " + filePathRelativeOfTestResources, ex);
    }
  }

  public static Path absulteFilePath(String filePathRelativeOfTestResources) {
    return Paths.get(
      executionPath(),
      "src",
      "test",
      "resources"
    ).resolve(filePathRelativeOfTestResources);
  }

  private static String executionPath() {
    try {
      return new File(".").getCanonicalPath();
    } catch (IOException ex) {
      throw new RuntimeException("Cannot establish execution path.", ex);
    }
  }
}
