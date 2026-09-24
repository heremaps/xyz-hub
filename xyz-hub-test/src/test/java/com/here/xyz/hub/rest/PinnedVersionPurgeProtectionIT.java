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
import static org.hamcrest.Matchers.equalTo;

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
  public void extensionsNotPinnedToThisSpaceDoNotBlockTheDeletion() {
    //Create a base space with 3 versions, and an extension pinned to version 1
    newExtension(newBase(1), 1L);
    String base = newBase(3);
    //Create an extension that is not pinned to any version of the base space
    newExtension(base, null);
    //Deleting changesets below version 3 is allowed, since the extension is not pinned to any version of the base space
    deleteChangesets(base, 3).statusCode(NO_CONTENT.code());
  }

  @Test
  public void theOldestAvailableVersionFollowsPinsAndTags() {
    String base = newBase(4);
    String delta = newExtension(base, 2L);
    //Create a tag at version 3, and set the rolling window to 2 versions
    given().contentType(APPLICATION_JSON).headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new JsonObject().put("id", "tag-v3").put("version", 3).encode())
        .when().post(getSpacesPath() + "/" + base + "/tags")
        .then().statusCode(OK.code());
    //The oldest available version is now 2, since the extension is pinned to version 2 and the tag is at version 3
    patchSpace(base, new JsonObject().put("versionsToKeep", 2)).statusCode(OK.code());

    //The oldest available version is now 2, since the extension is pinned to version 2 and the tag is at version 3
    statistics(base).body("minVersion.value", equalTo(2));

    //Removing the extension allows the deletion to proceed, and the oldest available version is now 3,
    //since the tag is at version 3 and the rolling window starts at version 3
    removeSpace(delta);
    spaces.remove(delta);
    //The oldest available version is now 3, since the tag is at version 3 and the rolling window starts at version 3
    statistics(base).body("minVersion.value", equalTo(3));
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

  private void assertKey1(String space, String expected) {
    given().headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when().get(getSpacesPath() + "/" + space + "/iterate")
        .then().statusCode(OK.code())
        .body("features.find { it.id == 'f1' }.properties.key1", equalTo(expected));
  }
}
