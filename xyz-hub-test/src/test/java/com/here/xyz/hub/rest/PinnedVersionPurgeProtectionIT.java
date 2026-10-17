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

package com.here.xyz.hub.rest;

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;

import com.here.xyz.models.geojson.implementation.Properties;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class PinnedVersionPurgeProtectionIT extends TestSpaceWithFeature {

  private final List<String> spaces = new ArrayList<>();

  @AfterEach
  public void cleanup() {
    for (int i = spaces.size() - 1; i >= 0; i--) {
      removeSpace(spaces.get(i));
    }
    spaces.clear();
  }

  @Test
  public void deletingChangesetsBelowAPinKeepsThePinnedVersion() {
    String base = newBase(3);
    String delta = newExtension(base, 2L);

    //The extension is pinned to version 2, so deleting changesets below version 3 only deletes below version 2
    deleteChangesets(base, 3).statusCode(NO_CONTENT.code());
    //Check that the pinned version is still readable through the extension
    assertKey1(delta, "v2");
  }

  @Test
  public void rePinningAndUnpinningMoveTheProtectedVersion() {
    String base = newBase(5);
    String delta = newExtension(base, 2L);

    //The rolling window alone would keep the versions 4 and 5, but the pin of the extension keeps version 2
    patchSpace(base, new JsonObject().put("versionsToKeep", 2));
    statistics(base).body("minVersion.value", equalTo(2));

    //Moving the pin to version 3 releases version 2
    patchSpace(delta, new JsonObject().put("extends", new JsonObject().put("spaceId", base).put("version", 3)));
    tags(base, true).body("size()", equalTo(1)).body("[0].version", equalTo(3));
    statistics(base).body("minVersion.value", equalTo(3));

    //Removing the pin releases version 3 as well
    patchSpace(delta, new JsonObject().put("extends", new JsonObject().put("spaceId", base).putNull("version")));
    statistics(base).body("minVersion.value", equalTo(4));
  }

  @Test
  public void eachExtensionKeepsItsBaseVersionWithAHiddenSystemTag() {
    String base = newBase(5);
    String lowerDelta = newExtension(base, 2L);
    String higherDelta = newExtension(base, 3L);

    //Each extension has its own system tag, which is hidden from the normal tag listing
    tags(base, false).body("size()", equalTo(0));
    tags(base, true)
        .body("size()", equalTo(2))
        .body("system", everyItem(equalTo(true)))
        .body("description", hasItem(containsString(lowerDelta)));

    //The rolling window alone would keep the versions 4 and 5, but the lower of the two tags wins
    patchSpace(base, new JsonObject().put("versionsToKeep", 2));
    statistics(base).body("minVersion.value", equalTo(2));

    //Deleting an extension removes only its own tag
    removeSpace(lowerDelta);
    spaces.remove(lowerDelta);
    statistics(base).body("minVersion.value", equalTo(3));
    assertKey1(higherDelta, "v3");

    //Once the last tag is gone, the rolling window applies again
    removeSpace(higherDelta);
    spaces.remove(higherDelta);
    tags(base, true).body("size()", equalTo(0));
    statistics(base).body("minVersion.value", equalTo(4));
  }

  @Test
  public void dryRunsDoNotChangeThePins() {
    String base = newBase(3);
    String delta = newExtension(base, 2L);

    given().contentType(APPLICATION_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new JsonObject().put("title", "dry-run").put("extends", new JsonObject().put("spaceId", base).put("version", 1)).encode())
        .when().post(getSpacesPath() + "?dryRun=true")
        .then().statusCode(OK.code());
    given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().delete(getSpacesPath() + "/" + delta + "?dryRun=true")
        .then().statusCode(NO_CONTENT.code());

    //Still only the pin of the real extension
    tags(base, true)
        .body("size()", equalTo(1))
        .body("[0].version", equalTo(2));
  }

  private String newBase(int versions) {
    String id = newSpace(null);
    for (int v = 1; v <= versions; v++) {
      given().contentType(APPLICATION_GEO_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
          .body(newFeature("f1").withProperties(new Properties().with("key1", "v" + v)).serialize())
          .when().post(getSpacesPath() + "/" + id + "/features")
          .then().statusCode(OK.code());
    }
    return id;
  }

  private String newExtension(String base, Long pin) {
    JsonObject extension = new JsonObject().put("spaceId", base);
    if (pin != null) {
      extension.put("version", pin);
    }
    return newSpace(extension);
  }

  private String newSpace(JsonObject extension) {
    String id = "pin-purge-it-" + UUID.randomUUID();
    JsonObject space = new JsonObject().put("id", id).put("title", id).put("versionsToKeep", 100);
    if (extension != null) {
      space.put("extends", extension);
    }
    spaces.add(id);

    given().contentType(APPLICATION_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(space.encode())
        .when().post(getCreateSpacePath())
        .then().statusCode(OK.code());
    return id;
  }

  private ValidatableResponse deleteChangesets(String space, long belowVersion) {
    return given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().delete(getSpacesPath() + "/" + space + "/changesets?version=lt=" + belowVersion)
        .then();
  }

  private ValidatableResponse statistics(String space) {
    return given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().get(getSpacesPath() + "/" + space + "/statistics")
        .then().statusCode(OK.code());
  }

  private ValidatableResponse tags(String space, boolean includeSystemTags) {
    return given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().get(getSpacesPath() + "/" + space + "/tags?includeSystemTags=" + includeSystemTags)
        .then().statusCode(OK.code());
  }

  private void assertKey1(String space, String expected) {
    given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().get(getSpacesPath() + "/" + space + "/iterate")
        .then().statusCode(OK.code())
        .body("features.find { it.id == 'f1' }.properties.key1", equalTo(expected));
  }
}
