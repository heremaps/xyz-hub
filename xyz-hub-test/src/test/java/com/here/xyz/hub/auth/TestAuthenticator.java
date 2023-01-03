/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import com.google.common.io.ByteStreams;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestAuthenticator {

  protected static Map<String, String> getAuthHeaders(AuthProfile authProfile) {
    HashMap<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer " + authProfile.jwt_string);
    return authHeaders;
  }

  protected static String content(String file) {
    try {
      return new String(ByteStreams.toByteArray(TestAuthenticator.class.getResourceAsStream(file))).trim();
    }
    catch (IOException e) {
      throw new RuntimeException("Error while reading token from resource file: " + file, e);
    }
  }

  protected static String content(String file, String storageId) {
    try {
      String content = new String(ByteStreams.toByteArray(TestAuthenticator.class.getResourceAsStream(file))).trim();
      JsonObject jsonContent = new JsonObject(content);

      JsonObject storage = new JsonObject().put("id", storageId);
      jsonContent.put("storage", storage);
      return jsonContent.encode();
    }
    catch (IOException e) {
      throw new RuntimeException("Error while reading token from resource file: " + file, e);
    }
  }

  public enum AuthProfile {
    NO_ACCESS,
    ACCESS_ALL,
    ACCESS_OWNER_1_ADMIN,
    ACCESS_OWNER_1_NO_ADMIN,
    ACCESS_OWNER_1_WITH_LIMITS,
    ACCESS_OWNER_1_WITH_FEATURES_ONLY,
    ACCESS_OWNER_2,
    ACCESS_OWNER_2_ALL,
    STORAGE_AUTH_TEST_C1_ONLY,
    STORAGE_AUTH_TEST_C1_OWNER_AND_ID,
    STORAGE_AUTH_TEST_C2_OWNER_AND_ID,
    STORAGE_AUTH_TEST_C2_OTHER_OWNER_AND_ID,
    STORAGE_AUTH_TEST_C3_OWNER_AND_ID,
    STORAGE_AUTH_TEST_C3_OTHER_OWNER_AND_ID,
    STORAGE_AUTH_TEST_OTHER_OWNER_ID_ONLY,
    STORAGE_AUTH_TEST_OWNER_ID_ONLY,
    STORAGE_AUTH_TEST_PSQL_ONLY,
    CONNECTOR_AUTH_TEST_C1_AND_C2,
    ACCESS_OWNER_1_READ_PACKAGES_HERE,
    ACCESS_OWNER_1_READ_WRITE_PACKAGES_HERE,
    ACCESS_OWNER_1_MANAGE_PACKAGES_HERE,
    ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_OSM,
    ACCESS_OWNER_1_MANAGE_PACKAGES_HERE_WITH_OWNER,
    ACCESS_OWNER_2_MANAGE_PACKAGES_HERE_OSM,
    ACCESS_OWNER_1_WITH_LISTENER,
    ACCESS_OWNER_1_WITH_ANOTHER_LISTENER,
    ACCESS_OWNER_1_WITH_PSQL,
    ACCESS_OWNER_1_WITH_ACCESS_CONNECTOR_RULE_TAGGER,
    ACCESS_OWNER_1_WITH_USE_CAPABILITIES,
    ACCESS_OWNER_1_WITH_USE_CAPABILITIES_AND_ADMIN,
    ACCESS_OWNER_1_WITH_MANAGE_SPACES_PACKAGE_HERE,
    ACCESS_OWNER_1_WITH_MANAGE_OWN_CONNECTORS,
    ACCESS_OWNER_1_WITH_MANAGE_CONNECTORS_WITH_PREFIX_ID,
    ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_ONE_ID,
    ACCESS_OWNER_1_WITH_MANAGE_CONNECTOR_PSQL,
    ACCESS_OWNER_2_WITH_MANAGE_CONNECTORS,
    ACCESS_OWNER_1_WITH_MS_PACKAGE_HERE_AND_MP_OSM,
    ACCESS_ADMIN_MESSAGING,
    ACCESS_ADMIN_STATISTICS,
    ACCESS_OWNER_1_WITH_FEATURES_MANAGE_ALL_SPACES,
    ACCESS_OWNER_1_MANAGE_ALL_SPACES_ONLY,
    ACCESS_OWNER_2_WITH_FEATURES_ADMIN_ALL_SPACES,
    ACCESS_OWNER_3,
    ACCESS_OWNER_3_WITH_CUSTOM_SPACE_IDS,
    ACCESS_OWNER_1_READ_ALL_FEATURES,
    ACCESS_SPACE_1_MANAGE_SPACES,
    ACCESS_SPACE_2_MANAGE_SPACES;

    public final String jwt_string;
    public final JWTPayload payload;

    AuthProfile() {
      String resourceFilename = "/auth/" + name() + ".json";
      this.payload = JwtGenerator.readTokenPayload(resourceFilename);
      jwt_string = JwtGenerator.generateToken(this.payload);
    }
  }
}
