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

package com.here.xyz.hub.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.io.ByteStreams;
import com.here.xyz.hub.NakshaHubOpenApiVerticle;
import org.junit.Test;

public class OpenApiGeneratorTest {

  @Test
  public void validYamlOutputTest() throws Exception {
    byte[] result = OpenApiGenerator.generate(new byte[0], new byte[0]);
    assertNotNull(result);
    assertTrue("Expected result not empty", result.length > 0);
  }

  @Test
  public void generateStableTest() throws Exception {
    final byte[] openapiSource = ByteStreams.toByteArray(NakshaHubOpenApiVerticle.class.getResourceAsStream("/openapi.yaml"));
    final byte[] stableRecipe = ByteStreams.toByteArray(NakshaHubOpenApiVerticle.class.getResourceAsStream("/recipes/openapi-recipe-stable.yaml"));
    byte[] result = OpenApiGenerator.generate(openapiSource, stableRecipe);
    assertNotNull(result);
    assertTrue("Expected result not empty", result.length > 0);
  }

  @Test
  public void generateExperimentalTest() throws Exception {
    final byte[] openapiSource = ByteStreams.toByteArray(NakshaHubOpenApiVerticle.class.getResourceAsStream("/openapi.yaml"));
    final byte[] experimentalRecipe = ByteStreams.toByteArray(NakshaHubOpenApiVerticle.class.getResourceAsStream("/recipes/openapi-recipe-experimental.yaml"));
    byte[] result = OpenApiGenerator.generate(openapiSource, experimentalRecipe);
    assertNotNull(result);
    assertTrue("Expected result not empty", result.length > 0);
  }

  @Test
  public void generateContractTest() throws Exception {
    final byte[] openapiSource = ByteStreams.toByteArray(NakshaHubOpenApiVerticle.class.getResourceAsStream("/openapi.yaml"));
    final byte[] contractRecipe = ByteStreams.toByteArray(NakshaHubOpenApiVerticle.class.getResourceAsStream("/recipes/openapi-recipe-contract.yaml"));
    byte[] result = OpenApiGenerator.generate(openapiSource, contractRecipe);
    assertNotNull(result);
    assertTrue("Expected result not empty", result.length > 0);
  }
}
