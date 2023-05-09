/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.responses.XyzError;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import java.io.IOException;
import org.junit.Test;

@SuppressWarnings("unused")
public class JsonMappingTest {

  @Test
  public void testDeserializeFeature() throws Exception {
    final String json = "{\"type\":\"Feature\", \"id\": \"xyz123\", \"properties\":{\"x\":5}, \"otherProperty\": \"123\"}";
    final Feature obj = new ObjectMapper().readValue(json, Feature.class);
    assertNotNull(obj);

    assertEquals(5, (int) obj.getProperties().get("x"));
    assertEquals("123", obj.get("otherProperty"));
  }

  @Test
  public void testSerializeFeature() throws Exception {
    final String json = "{\"type\":\"Feature\", \"id\": \"xyz123\", \"properties\":{\"x\":5}}";
    final Feature obj = new ObjectMapper().readValue(json, Feature.class);
    assertNotNull(obj);

    obj.getProperties().put("y", 7);
    String result = obj.serialize();

    final String string1 = "{\"type\":\"Feature\",\"id\":\"xyz123\",\"properties\":{\"@ns:com:here:xyz\":{\"createdAt\":0,\"updatedAt\":0,\"version\":0,\"deleted\":false},\"x\":5,\"y\":7}}";
    assertTrue(jsonCompare(string1, result));
  }

  private boolean jsonCompare(@SuppressWarnings("SameParameterValue") String string1, String string2) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode tree1 = mapper.readTree(string1);
    JsonNode tree2 = mapper.readTree(string2);
    return tree1.equals(tree2);
  }

  @Test
  public void testResponseParsing() throws Exception {
    final String json = "{\"type\":\"ErrorResponse\",\"error\":\"NotImplemented\",\"errorMessage\":\"Hello World!\"}";
    final ErrorResponse obj = new ObjectMapper().readValue(json, ErrorResponse.class);
    assertNotNull(obj);
    assertSame(XyzError.NOT_IMPLEMENTED, obj.getError());
    assertEquals("Hello World!", obj.getErrorMessage());
  }

  @Test
  public void testNativeAWSLambdaErrorMessage() throws Exception {
    final String json = "{\"errorMessage\":\"2018-09-15T07:12:25.013Z a368c0ea-b8b6-11e8-b894-eb5a7755e998 Task timed out after 25.01 seconds\"}";
    ErrorResponse obj = new ErrorResponse();
    obj = new ObjectMapper().readerForUpdating(obj).readValue(json);
    assertNotNull(obj);
    obj = XyzSerializable.fixAWSLambdaResponse(obj);
    assertSame(XyzError.TIMEOUT, obj.getError());
    assertEquals("2018-09-15T07:12:25.013Z a368c0ea-b8b6-11e8-b894-eb5a7755e998 Task timed out after 25.01 seconds",
        obj.getErrorMessage());
  }

}