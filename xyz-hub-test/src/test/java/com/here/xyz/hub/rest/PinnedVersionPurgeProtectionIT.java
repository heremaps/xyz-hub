/*
 * Copyright (C) 2026 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import com.here.xyz.models.geojson.implementation.Properties;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Checks that a version which a version-bound (pinned) extension is reading survives the deletion of changesets, and
 * that the oldest available version which is reported stays consistent with what is physically there.
 *
 * <p>The rolling {@code versionsToKeep} purge inside the connector is only sampled every 1000th version, so it is not
 * practical to trigger here. These tests therefore drive the explicit purge, which is the path a user can trigger at
 * any time, plus the reported {@code minVersion} that new pins are validated against.
 */
public class PinnedVersionPurgeProtectionIT extends TestSpaceWithFeature {

  private final List<String> createdSpaces = new ArrayList<>();

  @AfterEach
  public void cleanup() {
    for (int i = createdSpaces.size() - 1; i >= 0; i--)
      removeSpace(createdSpaces.get(i));
    createdSpaces.clear();
  }

  @Test
  public void deletingChangesetsBelowAPinnedVersionIsRejected() {
    String base = newSpace(100, null, null);
    write(base, "f1", "base-v1"); //version 1
    write(base, "f1", "base-v2"); //version 2
    write(base, "f1", "base-v3"); //version 3
    String delta = newSpace(100, base, 2L);

    deleteChangesets(base, 3)
        .statusCode(BAD_REQUEST.code())
        .body("errorMessage", containsString(delta));

    //The pinned version must still be readable, both directly and through the extension
    assertFeature(base, "f1", "base-v2", "2");
    assertFeature(delta, "f1", "base-v2", null);
  }

  @Test
  public void deletingChangesetsUpToThePinnedVersionIsAllowed() {
    String base = newSpace(100, null, null);
    write(base, "f1", "base-v1");
    write(base, "f1", "base-v2");
    write(base, "f1", "base-v3");
    String delta = newSpace(100, base, 2L);

    //Everything below the pinned version may go, the pinned version itself is kept
    deleteChangesets(base, 2).statusCode(NO_CONTENT.code());

    assertFeature(delta, "f1", "base-v2", null);
  }

  @Test
  public void anUnpinnedExtensionDoesNotBlockTheDeletionOfChangesets() {
    String base = newSpace(100, null, null);
    write(base, "f1", "base-v1");
    write(base, "f1", "base-v2");
    write(base, "f1", "base-v3");
    newSpace(100, base, null);

    deleteChangesets(base, 3).statusCode(NO_CONTENT.code());
  }

  @Test
  public void removingThePinnedExtensionUnblocksTheDeletionOfChangesets() {
    String base = newSpace(100, null, null);
    write(base, "f1", "base-v1");
    write(base, "f1", "base-v2");
    write(base, "f1", "base-v3");
    String delta = newSpace(100, base, 2L);

    deleteChangesets(base, 3).statusCode(BAD_REQUEST.code());

    removeSpace(delta);
    createdSpaces.remove(delta);

    deleteChangesets(base, 3).statusCode(NO_CONTENT.code());
  }

  @Test
  public void anExtensionOfAnotherSpaceDoesNotBlockTheDeletionOfChangesets() {
    String unrelatedBase = newSpace(100, null, null);
    write(unrelatedBase, "f1", "unrelated-v1");
    newSpace(100, unrelatedBase, 1L);

    String base = newSpace(100, null, null);
    write(base, "f1", "base-v1");
    write(base, "f1", "base-v2");
    write(base, "f1", "base-v3");

    deleteChangesets(base, 3).statusCode(NO_CONTENT.code());
  }

  /** A pin keeps its version alive past the rolling window, so it has to be reported as the oldest available one. */
  @Test
  public void aPinnedVersionIsReportedAsTheOldestAvailableVersion() {
    String base = newSpace(100, null, null);
    write(base, "f1", "base-v1");
    write(base, "f1", "base-v2");
    write(base, "f1", "base-v3");
    write(base, "f1", "base-v4");
    newSpace(100, base, 2L);

    //Shrink the rolling window so that it would no longer include the pinned version
    patchSpace(base, new JsonObject().put("versionsToKeep", 2)).statusCode(OK.code());

    statistics(base).body("minVersion.value", equalTo(2));
  }

  /** A tag and a pin both protect their version, so the lower of the two is the oldest available one. */
  @Test
  public void aPinBelowATagIsReportedAsTheOldestAvailableVersion() {
    String base = newSpace(100, null, null);
    write(base, "f1", "base-v1");
    write(base, "f1", "base-v2");
    write(base, "f1", "base-v3");
    write(base, "f1", "base-v4");
    newSpace(100, base, 2L);
    given().contentType(APPLICATION_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new JsonObject().put("id", "tag-v3").put("version", 3).encode())
        .when().post(getSpacesPath() + "/" + base + "/tags")
        .then().statusCode(OK.code());

    patchSpace(base, new JsonObject().put("versionsToKeep", 2)).statusCode(OK.code());

    statistics(base).body("minVersion.value", equalTo(2));
  }

  @Test
  public void withoutAnyProtectionTheRollingWindowDefinesTheOldestAvailableVersion() {
    String base = newSpace(100, null, null);
    write(base, "f1", "base-v1");
    write(base, "f1", "base-v2");
    write(base, "f1", "base-v3");
    write(base, "f1", "base-v4");

    patchSpace(base, new JsonObject().put("versionsToKeep", 2)).statusCode(OK.code());

    //Control for the test above: with nothing pinned, only the 2 newest versions are reported as available
    statistics(base).body("minVersion.value", equalTo(3));
  }

  // --- helpers -----------------------------------------------------------------------------------------------------

  private String newSpace(int history, String base, Long pin) {
    String id = "pin-purge-it-" + UUID.randomUUID();
    Map<String, Object> definition = new HashMap<>(Map.of("id", id, "title", "Pinned purge protection", "versionsToKeep", history));
    if (base != null) {
      Map<String, Object> extension = new HashMap<>(Map.of("spaceId", base));
      if (pin != null)
        extension.put("version", pin.toString());
      definition.put("extends", extension);
    }
    createdSpaces.add(id);

    given().contentType(APPLICATION_JSON).accept(APPLICATION_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new JsonObject(definition).encode())
        .when().post(getCreateSpacePath())
        .then().statusCode(OK.code());
    return id;
  }

  private void write(String space, String id, String value) {
    given().contentType(APPLICATION_GEO_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(newFeature(id).withProperties(new Properties().with("key1", value)).serialize())
        .when().post(getSpacesPath() + "/" + space + "/features")
        .then().statusCode(OK.code());
  }

  private ValidatableResponse deleteChangesets(String space, long belowVersion) {
    return given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().delete(getSpacesPath() + "/" + space + "/changesets?version=lt=" + belowVersion)
        .then();
  }

  private ValidatableResponse statistics(String space) {
    return given().contentType(APPLICATION_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().get(getSpacesPath() + "/" + space + "/statistics")
        .then().statusCode(OK.code());
  }

  private void assertFeature(String space, String id, String key1, String versionRef) {
    String path = "features.find { it.id == '" + id + "' }.properties";
    given().contentType(APPLICATION_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .queryParam("context", "DEFAULT").queryParam("versionRef", versionRef == null ? "HEAD" : versionRef)
        .when().get(getSpacesPath() + "/" + space + "/iterate")
        .then().statusCode(OK.code())
        .body("features.findAll { it.id == '" + id + "' }", hasSize(1))
        .body(path + ".key1", equalTo(key1));
  }
}
