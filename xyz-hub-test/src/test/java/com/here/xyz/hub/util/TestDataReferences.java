package com.here.xyz.hub.util;

import com.here.xyz.models.hub.DataReference;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

final class TestDataReferences {

  private TestDataReferences() {
    // utility class
  }

  static DataReference dataReference(UUID referenceId) {
    return new DataReference()
      .withId(referenceId)
      .withEntityId("entityId-A")
      .withPatch(true)
      .withStartVersion(1)
      .withEndVersion(5)
      .withObjectType("object-type-A")
      .withContentType("content-type-A")
      .withLocation("location-A")
      .withSourceSystem("source-system-A")
      .withTargetSystem("target-system-A");
  }

  static Map<String, Object> dataReferenceAsMap(UUID referenceId) {
    Map<String, Object> dataReference = new HashMap<>();
    dataReference.put("id", referenceId.toString());
    dataReference.put("entityId", "entityId-A");
    dataReference.put("isPatch", true);
    dataReference.put("startVersion", new BigDecimal(1));
    dataReference.put("endVersion", new BigDecimal(5));
    dataReference.put("objectType", "object-type-A");
    dataReference.put("contentType", "content-type-A");
    dataReference.put("location", "location-A");
    dataReference.put("sourceSystem", "source-system-A");
    dataReference.put("targetSystem", "target-system-A");

    return dataReference;
  }

}
