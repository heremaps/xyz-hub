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

package com.here.xyz;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.here.xyz.events.CountFeaturesEvent;
import com.here.xyz.events.EventNotification;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.responses.XyzError;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import java.io.IOException;
import org.junit.Test;

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

    final String string1 = "{\"id\":\"xyz123\",\"properties\":{\"x\":5,\"y\":7},\"type\":\"Feature\"}";
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

  @Test
  public void test_map() throws Exception {
    final String json = "{\"type\":\"CountFeaturesEvent\", \"space\":\"test\"}";
    CountFeaturesEvent obj = new ObjectMapper().readValue(json, CountFeaturesEvent.class);
    assertNotNull(obj);
  }


  //@Test
  //TODO: Please change the URL to some file:/// URL
  public void parseTest() throws Exception {
    final String json = "{ \"type\": \"EventNotification\", \"eventType\": \"ModifyFeaturesEvent.request\", \"event\": { \"type\": \"ModifyFeaturesEvent\", \"space\": \"foo\", \"params\": { \"schemaUrl\": \"file:///someSchema.json\" }, \"insertFeatures\": [ { \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 14.3222, -2.32506 ] }, \"type\": \"Feature\", \"properties\": { \"name\": \"Toyota\", \"@ns:com:here:xyz\": { \"tags\": [ \"yellow\" ] } } }, { \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 14.3222, -2.32506 ] }, \"type\": \"Feature\", \"properties\": { \"name\": \"Tesla\", \"@ns:com:here:xyz\": { \"tags\": [ \"red\" ] } } } ] } }";
    EventNotification obj = new ObjectMapper().readValue(json, EventNotification.class);
    new ObjectMapper().convertValue(obj.getEvent(), ModifyFeaturesEvent.class);
  }

  @Test
  public void testSpaceWithListenersAsList() throws Exception {
    ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
    Space space = mapper.readValue(JsonMappingTest.class.getResourceAsStream("test/SpaceWithListenersAsList.json"), Space.class);

    assertNotNull(space);

    assertNotNull(space.getProcessors());
    assertEquals(1, space.getProcessors().size());
    Space.ListenerConnectorRef p = space.getProcessors().get("rule-tagger").get(0);
    assertEquals(Integer.valueOf(0), p.getOrder());
    assertEquals(0, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    p = space.getProcessors().get("rule-tagger").get(1);
    assertEquals(Integer.valueOf(1), p.getOrder());
    assertEquals(1, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    assertNotNull(space.getListeners());
    assertEquals(2, space.getListeners().size());
    p = space.getListeners().get("schema-validator").get(0);
    assertEquals(Integer.valueOf(0), p.getOrder());
    assertEquals(0, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    p = space.getListeners().get("test").get(0);
    assertEquals(Integer.valueOf(1), p.getOrder());
    assertEquals(1, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    //System.out.println(mapper.writeValueAsString(space));
  }

  @Test
  public void testSpaceWithListenersAsMap() throws Exception {
    ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
    Space space = mapper.readValue(JsonMappingTest.class.getResourceAsStream("test/SpaceWithListenersAsMap.json"), Space.class);

    assertNotNull(space);

    assertNotNull(space.getProcessors());
    assertEquals(1, space.getProcessors().size());
    Space.ListenerConnectorRef p = space.getProcessors().get("rule-tagger").get(0);
    assertNull(space.getProcessors().get("rule-tagger").get(0).getId());
    assertEquals(Integer.valueOf(2), p.getOrder());
    assertEquals(2, p.getParams().get("not_the_real_order"));
    p = space.getProcessors().get("rule-tagger").get(1);
    assertEquals(Integer.valueOf(0), p.getOrder());
    assertEquals(0, p.getParams().get("not_the_real_order"));
    p = space.getProcessors().get("rule-tagger").get(2);
    assertEquals(Integer.valueOf(1), p.getOrder());
    assertEquals(1, p.getParams().get("not_the_real_order"));

    assertNotNull(space.getListeners());
    assertEquals(2, space.getListeners().size());
    p = space.getListeners().get("test").get(0);
    assertEquals(Integer.valueOf(0), p.getOrder());
    assertEquals(0, p.getParams().get("not_the_real_order"));
    p = space.getListeners().get("schema-validator").get(0);
    assertEquals(Integer.valueOf(1), p.getOrder());
    assertEquals(1, p.getParams().get("not_the_real_order"));
    //System.out.println(mapper.writeValueAsString(space));
  }

  @Test
  public void testSpaceWithListenersInStrangeOrder() throws Exception {
    ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
    Space space = mapper.readValue(JsonMappingTest.class.getResourceAsStream("test/SpaceWithListenersInStrangeOrder.json"), Space.class);
    assertNotNull(space);

    assertNotNull(space.getListeners());
    assertTrue(space.getListeners().isEmpty());

    assertNotNull(space.getProcessors());
    Space.ListenerConnectorRef p = space.getProcessors().get("rule-tagger").get(0);
    assertEquals(Integer.valueOf(0), p.getOrder());
    assertEquals(0, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    p = space.getProcessors().get("schema-validator").get(0);
    assertEquals(Integer.valueOf(1), p.getOrder());
    assertEquals(1, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    p = space.getProcessors().get("rule-tagger").get(1);
    assertEquals(Integer.valueOf(2), p.getOrder());
    assertEquals(2, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    p = space.getProcessors().get("test1").get(0);
    assertEquals(Integer.valueOf(3), p.getOrder());
    assertEquals(3, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    p = space.getProcessors().get("test2").get(0);
    assertEquals(Integer.valueOf(4), p.getOrder());
    assertEquals(4, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    p = space.getProcessors().get("test1").get(1);
    assertEquals(Integer.valueOf(5), p.getOrder());
    assertEquals(5, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

    p = space.getProcessors().get("test3").get(0);
    assertEquals(Integer.valueOf(6), p.getOrder());
    assertEquals(6, p.getParams().get("not_the_real_order"));
    assertNull(p.getId());

  }

  @Test
  public void testConvert() throws IOException {
    ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
    JsonNode node = mapper.readValue(JsonMappingTest.class.getResourceAsStream("test/SpaceWithListenersAsMap.json"), JsonNode.class);
    Space space = mapper.convertValue(node, Space.class);
    System.out.println(mapper.writeValueAsString(space));
  }
  @Test
  public void testSpaceWithNullListeners() throws Exception {
    ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
    Space space = mapper.readValue(JsonMappingTest.class.getResourceAsStream("test/SpaceWithNullListeners.json"), Space.class);
    assertNotNull(space);
    assertNotNull(space.getListeners());
    assertTrue(space.getListeners().containsKey("schema-validator"));
    assertNull(space.getListeners().get("schema-validator"));
    assertNull(space.getProcessors());
    //System.out.println(mapper.writeValueAsString(space));
  }
}
