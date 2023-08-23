package com.here.xyz.psql;

import static org.junit.Assert.assertEquals;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PSQLSearchIT extends PSQLAbstractIT {

  private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<>() {};
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static String ORIGINAL_EVENT;

  @BeforeClass
  public static void init() throws Exception {
    PSQLAbstractIT.init();
    //Retrieve the original event
    ORIGINAL_EVENT = IOUtils.toString(PSQLSearchIT.class.getResourceAsStream("/events/BasicSearchByPropertiesAndTagsEvent.json"));
  }

  @Before
  public void createSpace() throws Exception {
    invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    //Prepare the data
    invokeLambdaFromFile("/events/InsertFeaturesForSearchTestEvent.json");
  }

  @After
  public void shutdown() throws Exception {
    invokeDeleteTestSpace(null);
  }

  @SafeVarargs
  protected final void addPropertiesQueryToSearchObject(Map<String, Object> json, boolean or, Map<String, Object>... objects) {
    if (!json.containsKey("propertiesQuery")) {
      json.put("propertiesQuery", new ArrayList<List<Map<String, Object>>>());
    }

    @SuppressWarnings({"unchecked", "rawtypes"}) final List<List<Map<String, Object>>> list = (List) json.get("propertiesQuery");
    if (or) {
      list.add(Arrays.asList(objects));
      return;
    }

    if (list.size() == 0) {
      list.add(Arrays.asList(objects));
      return;
    }

    list.get(0).addAll(Arrays.asList(objects));
  }

  protected void addTagsToSearchObject(Map<String, Object> json, String... tags) {
    json.remove("tags");
    json.put("tags", new ArrayList<String>());
    ((List) json.get("tags")).add(new ArrayList(Arrays.asList(tags)));
  }

  protected void invokeAndAssert(Map<String, Object> json, int size, String... names) throws Exception {
    String response = invokeLambda(new ObjectMapper().writeValueAsString(json));

    final FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    final List<Feature> responseFeatures = responseCollection.getFeatures();
    assertEquals("Check size", size, responseFeatures.size());

    for (int i = 0; i < size; i++) {
      assertEquals("Check name", names[i], responseFeatures.get(i).getProperties().get("name"));
    }
  }

  @Test
  public void test1() throws Exception { //TODO: rename
    // Test 1
    Map<String, Object> test1 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties1 = new HashMap<>();
    properties1.put("key", "properties.name");
    properties1.put("operation", "EQUALS");
    properties1.put("values", Collections.singletonList("Toyota"));
    addPropertiesQueryToSearchObject(test1, false, properties1);
    addTagsToSearchObject(test1, "yellow");
    invokeAndAssert(test1, 1, "Toyota");
  }

  @Test
  public void test2() throws Exception { //TODO: rename
    // Test 2
    Map<String, Object> test2 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties2 = new HashMap<>();
    properties2.put("key", "properties.size");
    properties2.put("operation", "LESS_THAN_OR_EQUALS");
    properties2.put("values", Collections.singletonList(1));
    addPropertiesQueryToSearchObject(test2, false, properties2);
    invokeAndAssert(test2, 2, "Ducati", "BikeX");
  }

  @Test
  public void test3() throws Exception { //TODO: rename
    // Test 3
    Map<String, Object> test3 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties3 = new HashMap<>();
    properties3.put("key", "properties.car");
    properties3.put("operation", "EQUALS");
    properties3.put("values", Collections.singletonList(true));
    addPropertiesQueryToSearchObject(test3, false, properties3);
    invokeAndAssert(test3, 1, "Toyota");
  }

  @Test
  public void test4() throws Exception { //TODO: rename
    // Test 4
    Map<String, Object> test4 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties4 = new HashMap<>();
    properties4.put("key", "properties.car");
    properties4.put("operation", "EQUALS");
    properties4.put("values", Collections.singletonList(false));
    addPropertiesQueryToSearchObject(test4, false, properties4);
    invokeAndAssert(test4, 1, "Ducati");
  }

  @Test
  public void test5() throws Exception { //TODO: rename
    // Test 5
    Map<String, Object> test5 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties5 = new HashMap<>();
    properties5.put("key", "properties.size");
    properties5.put("operation", "GREATER_THAN");
    properties5.put("values", Collections.singletonList(5));
    addPropertiesQueryToSearchObject(test5, false, properties5);
    addTagsToSearchObject(test5, "red");
    invokeAndAssert(test5, 1, "Toyota");
  }

  @Test
  public void test6() throws Exception { //TODO: rename
    // Test 6
    Map<String, Object> test6 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties6 = new HashMap<>();
    properties6.put("key", "properties.size");
    properties6.put("operation", "LESS_THAN");
    properties6.put("values", Collections.singletonList(5));
    addPropertiesQueryToSearchObject(test6, false, properties6);
    addTagsToSearchObject(test6, "red");
    invokeAndAssert(test6, 1, "Ducati");
  }

  @Test
  public void test7() throws Exception { //TODO: rename
    // Test 7
    Map<String, Object> test7 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties7 = new HashMap<>();
    properties7.put("key", "properties.name");
    properties7.put("operation", "EQUALS");
    properties7.put("values", Arrays.asList("Toyota", "Tesla"));
    addPropertiesQueryToSearchObject(test7,  false, properties7);
    invokeAndAssert(test7, 2, "Toyota", "Tesla");
  }

  @Test
  public void test8() throws Exception { //TODO: rename
    // Test 8
    Map<String, Object> test8 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    invokeAndAssert(test8, 4, "Toyota", "Tesla", "Ducati", "BikeX");
  }

  @Test
  public void test9() throws Exception { //TODO: rename
    // Test 9
    Map<String, Object> test9 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties9 = new HashMap<>();
    properties9.put("key", "properties.name");
    properties9.put("operation", "EQUALS");
    properties9.put("values", Collections.singletonList("Test"));
    addPropertiesQueryToSearchObject(test9, false, properties9);
    invokeAndAssert(test9, 0);
  }

  @Test
  public void test10() throws Exception { //TODO: rename
    // Test 10
    Map<String, Object> test10 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties10 = new HashMap<>();
    properties10.put("key", "properties.name");
    properties10.put("operation", "EQUALS");
    properties10.put("values", Collections.singletonList("Toyota"));
    addPropertiesQueryToSearchObject(test10, false, properties10);
    addTagsToSearchObject(test10, "cyan");
    invokeAndAssert(test10, 0);
  }

  @Test
  public void test11() throws Exception { //TODO: rename
    // Test 11
    Map<String, Object> test11 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties11_1 = new HashMap<>();
    properties11_1.put("key", "properties.name");
    properties11_1.put("operation", "EQUALS");
    properties11_1.put("values", Arrays.asList("Toyota", "Ducati", "BikeX"));
    Map<String, Object> properties11_2 = new HashMap<>();
    properties11_2.put("key", "properties.size");
    properties11_2.put("operation", "EQUALS");
    properties11_2.put("values", Arrays.asList(1D, 0.3D));
    addPropertiesQueryToSearchObject(test11, false, properties11_1, properties11_2);
    invokeAndAssert(test11, 2, "Ducati", "BikeX");
  }

  @Test
  public void test12() throws Exception { //TODO: rename
    // Test 12
    Map<String, Object> test12 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties12_1 = new HashMap<>();
    properties12_1.put("key", "properties.name");
    properties12_1.put("operation", "EQUALS");
    properties12_1.put("values", Arrays.asList("Toyota", "Ducati"));
    Map<String, Object> properties12_2 = new HashMap<>();
    properties12_2.put("key", "properties.name");
    properties12_2.put("operation", "EQUALS");
    properties12_2.put("values", Collections.singletonList("Toyota"));
    addPropertiesQueryToSearchObject(test12, false, properties12_1);
    addPropertiesQueryToSearchObject(test12, true, properties12_2);
    invokeAndAssert(test12, 2, "Toyota", "Ducati");
  }

  @Test
  public void test13() throws Exception { //TODO: rename
    // Test 13
    Map<String, Object> test13 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties13_1 = new HashMap<>();
    properties13_1.put("key", "properties.name");
    properties13_1.put("operation", "EQUALS");
    properties13_1.put("values", Collections.singletonList("Toyota"));
    Map<String, Object> properties13_2 = new HashMap<>();
    properties13_2.put("key", "properties.name");
    properties13_2.put("operation", "EQUALS");
    properties13_2.put("values", Collections.singletonList("Ducati"));
    addPropertiesQueryToSearchObject(test13, false, properties13_1);
    addPropertiesQueryToSearchObject(test13, true, properties13_2);
    invokeAndAssert(test13, 2, "Toyota", "Ducati");
  }

  @Test
  public void test14() throws Exception { //TODO: rename
    // Test 14
    Map<String, Object> test14 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties14_1 = new HashMap<>();
    properties14_1.put("key", "id");
    properties14_1.put("operation", "GREATER_THAN");
    properties14_1.put("values", Collections.singletonList(0));
    addPropertiesQueryToSearchObject(test14, false, properties14_1);

    String response = invokeLambda(MAPPER.writeValueAsString(test14));
    FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    List<Feature> responseFeatures = responseCollection.getFeatures();
    String id = responseFeatures.get(0).getId();
    assertEquals("Check size", 4, responseFeatures.size());
  }

  @Test
  public void test15() throws Exception { //TODO: rename
    Map<String, Object> test15 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    Map<String, Object> properties15_1 = new HashMap<>();
    properties15_1.put("key", "id");
    properties15_1.put("operation", "EQUALS");
    properties15_1.put("values", Collections.singletonList("F1"));
    addPropertiesQueryToSearchObject(test15, false, properties15_1);
    addPropertiesQueryToSearchObject(test15, true, properties15_1);

    String response = invokeLambda(MAPPER.writeValueAsString(test15));
    FeatureCollection responseCollection = XyzSerializable.deserialize(response);
    List<Feature> responseFeatures = responseCollection.getFeatures();
    assertEquals(1, responseFeatures.size());
  }

  @Test
  public void test16() throws Exception { //TODO: rename
    // Test 16
    Map<String, Object> test16 = MAPPER.readValue(ORIGINAL_EVENT, TYPE_REFERENCE);
    test16.put("type", "IterateFeaturesEvent");
    test16.put("handle", "1");
    invokeAndAssert(test16, 3, "Tesla", "Ducati", "BikeX");
  }
}
