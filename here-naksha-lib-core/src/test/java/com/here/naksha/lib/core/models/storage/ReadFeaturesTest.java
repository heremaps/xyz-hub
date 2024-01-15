package com.here.naksha.lib.core.models.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.lib.core.util.json.Json;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadFeaturesTest {

  @Test
  void testShallowCopy() throws JsonProcessingException {
    // given
    ReadFeatures readFeatures = new ReadFeatures();
    Json jsonGenerator = Json.get();

    // when
    String json = jsonGenerator.writer().writeValueAsString(readFeatures);

    // then
    String expectedJson = "{\"type\":\"ReadFeatures\",\"collections\":[],\"fetchSize\":1000,\"limit\":1000000,\"returnDeleted\":false}";
    assertEquals(expectedJson, json, "there is a property change in ReadFeatures, add it to shallowCopy and update json");
  }
}
