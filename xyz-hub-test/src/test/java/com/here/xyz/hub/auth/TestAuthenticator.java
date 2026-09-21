/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
import com.here.xyz.models.hub.jwt.JWTPayload;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestAuthenticator {

  /**
   * Appended to every space ID the tests create, so that test classes running in concurrent failsafe forks do not
   * address the same spaces. Failsafe passes the fork number in, running from an IDE it stays empty.
   */
  public static final String TEST_SUFFIX = System.getProperty("xyz.test.suffix", "");

  /** The spaces of the auth tests, fork local like every other test space. */
  public static final String AUTH_SPACE_ID = "x-auth-test-space" + TEST_SUFFIX;
  public static final String AUTH_SHARED_SPACE_ID = "x-auth-test-space-shared" + TEST_SUFFIX;

  /**
   * The connectors of the connector tests. Connectors are registered hub wide rather than per space,
   * so two forks creating and deleting them under the same ID would interfere.
   */
  public static final String CONNECTOR_ID = "test-connector" + TEST_SUFFIX;
  public static final String CONNECTOR_2_ID = "test-connector2" + TEST_SUFFIX;
  public static final String OTHER_CONNECTOR_ID = "xyz-connector" + TEST_SUFFIX;

  protected static Map<String, String> getAuthHeaders(AuthProfile authProfile) {
    HashMap<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer " + authProfile.jwt_string);
    return authHeaders;
  }

  protected static String content(String file) {
    return resolvePlaceholders(rawContent(file));
  }

  protected static String content(String file, String storageId) {
    JsonObject jsonContent = new JsonObject(rawContent(file));
    jsonContent.put("storage", new JsonObject().put("id", storageId));
    return resolvePlaceholders(jsonContent.encode());
  }

  private static String rawContent(String file) {
    try {
      return new String(ByteStreams.toByteArray(TestAuthenticator.class.getResourceAsStream(file))).trim();
    }
    catch (IOException e) {
      throw new RuntimeException("Error while reading token from resource file: " + file, e);
    }
  }

  /**
   * The spaces and connectors a token payload names explicitly. A grant has to keep matching the
   * space or connector the test works on, so these move with the fork suffix.
   *
   * The owner IDs deliberately stay as they are. Making them fork local would isolate the space
   * limits of a token, which the hub counts per owner, but the connectors c2 and c3 in the service
   * configuration name an owner as well, and that configuration is shared by every fork.
   */
  private static final String[] FORK_LOCAL_TOKEN_VALUES = {
      "space1",
      "space-2",
      "test-connector2"
  };

  /**
   * Rewrites a token payload so that it belongs to this fork alone. The values are replaced quoted,
   * so that only whole JSON values are hit and never a substring of a longer ID.
   */
  private static String forkLocalTokenPayload(String payloadJson) {
    if (TEST_SUFFIX.isEmpty())
      return payloadJson;

    for (String value : FORK_LOCAL_TOKEN_VALUES)
      payloadJson = payloadJson.replace("\"" + value + "\"", "\"" + value + TEST_SUFFIX + "\"");

    return payloadJson;
  }

  /**
   * Replaces the space ID placeholders of the test resources by the IDs which are actually in use in this JVM.
   */
  private static String resolvePlaceholders(String content) {
    if (!content.contains("${"))
      return content;
    return content
        .replace("${spaceId}", "x-psql-test" + TEST_SUFFIX)
        .replace("${extensibleSpaceId}", "x-psql-test-extensible" + TEST_SUFFIX)
        .replace("${extendingSpaceId}", "x-psql-extending-test" + TEST_SUFFIX)
        .replace("${authSharedSpaceId}", AUTH_SHARED_SPACE_ID)
        .replace("${authSpaceId}", AUTH_SPACE_ID)
        .replace("${connector2Id}", CONNECTOR_2_ID)
        .replace("${connectorId}", CONNECTOR_ID)
        .replace("${otherConnectorId}", OTHER_CONNECTOR_ID);
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
      this.payload = Json.decodeValue(forkLocalTokenPayload(rawContent(resourceFilename)), JWTPayload.class);
      jwt_string = JwtGenerator.generateToken(this.payload);
    }
  }
}
