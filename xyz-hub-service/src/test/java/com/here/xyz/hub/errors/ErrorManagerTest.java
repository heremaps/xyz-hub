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

package com.here.xyz.hub.errors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.errors.ErrorManager;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ErrorManagerTest {

  @BeforeAll
  public static void setUp() {
    ErrorManager.loadErrors("hub-errors.json");
    Map<String, String> defaults = new HashMap<>();
    defaults.put("featureContainerResource", "TestResource");
    defaults.put("versionRef", "v1.0");
    defaults.put("resourceId", "12345");
    ErrorManager.registerGlobalPlaceholders(defaults);
  }

  @Test
  public void testComposeWithoutPlaceholders_validErrorCode() {
    String errorCode = "E318441"; // "$resource not found"
    DetailedHttpException exception = new DetailedHttpException(errorCode);
    assertNotNull(exception);
    assertEquals(404, exception.status.code());
    String message = exception.getMessage();
    assertTrue(message.contains("TestResource not found"));
  }

  @Test
  public void testComposeWithPlaceholders_validErrorCode() {
    String errorCode = "E318404"; // "Invalid value for versionRef"
    Map<String, String> placeholders = new HashMap<>();
    placeholders.put("versionRef", "v2.0");

    String causeMessage = "the version does not exist";
    DetailedHttpException exception = new DetailedHttpException(errorCode, placeholders, new RuntimeException(causeMessage));
    assertNotNull(exception);
    assertEquals(400, exception.status.code());

    String errorResponseText = exception.errorDefinition.toErrorResponse(exception.placeholders).serialize();
    assertTrue(errorResponseText.contains("v2.0"));
    assertTrue(errorResponseText.contains(causeMessage));
  }

  @Test
  public void testComposeWithDefaultPlaceholders() {
    String errorCode = "E318441"; // "$resource not found"

    DetailedHttpException exception = new DetailedHttpException(errorCode);
    assertNotNull(exception);
    assertEquals(404, exception.status.code());

    String errorResponseText = exception.errorDefinition.toErrorResponse(exception.placeholders).serialize();
    assertTrue(errorResponseText.contains("12345"));
  }

  @Test
  public void testCompose_invalidErrorCode() {
    String unknownErrorCode = "UNKNOWN_CODE";
    assertThrows(IllegalArgumentException.class, () -> new DetailedHttpException(unknownErrorCode));
  }
}