/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

public class JWTPayloadTest {

  @Test
  public void fromJson() {
    String jwtString = "{\"aid\":\"XYZ-01234567-89ab-cdef-0123-456789aUSER1\",\"iat\":1521982864,\"exp\":1525039199,\"urm\":{\"xyz-hub\":{\"readFeatures\":[{\"owner\":\"XYZ-01234567-89ab-cdef-0123-456789aUSER1\"}]}}}";
    JsonObject jwtJson = new JsonObject(jwtString);
    JWTPayload jwtPayload = Json.mapper.convertValue(jwtJson,JWTPayload.class);
    final ActionMatrix hereActionMatrix = jwtPayload.urm.get("xyz-hub");
    assertTrue(hereActionMatrix.containsKey("readFeatures"));
  }

  @Test
  public void fromJsonWithLimit() {
    String jwtString = "{\"aid\":\"XYZ-01234567-89ab-cdef-0123-456789aUSER1\",\"iat\":1521982864,\"exp\":1525039199,\"limits\":{\"maxSpaces\":10,\"maxFeaturesPerSpace\":1000000},\"urm\":{\"xyz-hub\":{\"readFeatures\":[{\"owner\":\"XYZ-01234567-89ab-cdef-0123-456789aUSER1\"}]}}}";
    JsonObject jwtJson = new JsonObject(jwtString);
    JWTPayload jwtPayload = Json.mapper.convertValue(jwtJson,JWTPayload.class);
    assertTrue(jwtPayload.limits instanceof  XYZUsageLimits);
    assertEquals(10, jwtPayload.limits.maxSpaces);
    assertEquals(1_000_000, jwtPayload.limits.maxFeaturesPerSpace);
  }

  @Test
  public void fromJsonWithEmptyLimits() {
    String jwtString = "{\"aid\":\"XYZ-01234567-89ab-cdef-0123-456789aUSER1\",\"iat\":1521982864,\"exp\":1525039199,\"limits\":{},\"urm\":{\"xyz-hub\":{\"readFeatures\":[{\"owner\":\"XYZ-01234567-89ab-cdef-0123-456789aUSER1\"}]}}}";
    JsonObject jwtJson = new JsonObject(jwtString);
    JWTPayload jwtPayload = Json.mapper.convertValue(jwtJson,JWTPayload.class);
    assertTrue(jwtPayload.limits instanceof  XYZUsageLimits);
    assertEquals(-1, jwtPayload.limits.maxSpaces);
    assertEquals(-1, jwtPayload.limits.maxFeaturesPerSpace);
  }
}
