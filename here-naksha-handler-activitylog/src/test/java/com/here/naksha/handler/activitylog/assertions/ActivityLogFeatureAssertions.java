package com.here.naksha.handler.activitylog.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class ActivityLogFeatureAssertions {

  private final XyzFeature subject;

  private ActivityLogFeatureAssertions(XyzFeature subject) {
    this.subject = subject;
  }

  public static ActivityLogFeatureAssertions assertThatActivityLogFeature(XyzFeature xyzFeature) {
    assertNotNull(xyzFeature);
    return new ActivityLogFeatureAssertions(xyzFeature);
  }

  public ActivityLogFeatureAssertions hasId(String id) {
    Assertions.assertEquals(id, subject.getId());
    return this;
  }

  public ActivityLogFeatureAssertions hasActivityLogId(String id) {
    assertNotNull(subject.getProperties().getXyzActivityLog());
    Assertions.assertEquals(id, subject.getProperties().getXyzActivityLog().getId());
    return this;
  }

  public ActivityLogFeatureAssertions hasAction(String action) {
    assertNotNull(subject.getProperties().getXyzActivityLog());
    Assertions.assertEquals(action, subject.getProperties().getXyzActivityLog().getAction());
    return this;
  }

  public ActivityLogFeatureAssertions hasReversePatch(JsonNode reversePatch) {
    assertNotNull(subject.getProperties().getXyzActivityLog());
    Assertions.assertEquals(reversePatch, subject.getProperties().getXyzActivityLog().getDiff());
    return this;
  }

  public ActivityLogFeatureAssertions isIdenticalToDatahubSampleFeature(XyzFeature datahubFeature, String message) throws JSONException {
    alignDiff(subject);
    String subjectJson = JsonSerializable.serialize(subject);
    String datahubFeatureJson = JsonSerializable.serialize(datahubFeature);
    JSONAssert.assertEquals(message, datahubFeatureJson, subjectJson, JSONCompareMode.LENIENT);
    return this;
  }

  private static void alignDiff(XyzFeature xyzFeature) {
    JsonNode diff = xyzFeature.getProperties().getXyzActivityLog().getDiff();
    if(diff != null){
      ((ObjectNode) diff).put("copy", 0);
      ((ObjectNode) diff).put("move", 0);
    }
  }
}
