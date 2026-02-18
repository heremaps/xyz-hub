package com.here.xyz.models.hub;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
