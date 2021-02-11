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
package com.here.xyz.psql;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.*;
import com.here.xyz.models.geojson.coordinates.*;
import com.here.xyz.models.geojson.implementation.*;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.responses.StatisticsResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

public class PSQLReadIT extends PSQLAbstractIT {

    @BeforeClass
    public static void init() throws Exception { initEnv(null); }

    @Before
    public void removeTestSpaces() throws Exception { deleteTestSpace(null); }

    @After
    public void shutdown() throws Exception { shutdownEnv(null); }

    /**
     * Test all branches of the BBox query.
     */
    @Test
    public void testBBoxQuery() throws Exception {
        // =========== INSERT ==========
        final String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
        final String insertResponse = invokeLambdaFromFile(insertJsonFile);
        final String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        logger.info("Insert feature tested successfully");

        String queryEvent;
        // =========== QUERY BBOX ==========
        GetFeaturesByTileEvent getFeaturesByBBoxEvent
                = new GetFeaturesByTileEvent()
                    .withConnectorParams(defaultTestConnectorParams)
                    .withSpace("foo")
                    .withBbox(new BBox(-170, -170, 170, 170))
                    .withLimit(30000);

        String queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        FeatureCollection featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        List<Feature> features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(3, features.size());

        // =========== QUERY BBOX - +TAGS ==========
        getFeaturesByBBoxEvent
                = new GetFeaturesByTileEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTags(TagsQuery.fromQueryParameter(new ArrayList<String>(){{ add("yellow"); }}))
                .withBbox(new BBox(-170, -170, 170, 170));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());

        // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
        getFeaturesByBBoxEvent
                = new GetFeaturesByTileEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTags(TagsQuery.fromQueryParameter(new ArrayList<String>(){{ add("yellow"); }}))
                .withSelection(new ArrayList<String>(){{ add("id");add("type");add("geometry");add("properties.name");}})
                .withBbox(new BBox(-170, -170, 170, 170));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());
        assertEquals(1, new ObjectMapper().convertValue(features.get(0).getProperties(), Map.class).size());
        assertEquals("Toyota", features.get(0).getProperties().get("name"));
        assertNotNull(features.get(0).getId());
        assertNotNull(features.get(0));
        assertNotNull(features.get(0).getGeometry());

        // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
        getFeaturesByBBoxEvent
                = new GetFeaturesByTileEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTags(TagsQuery.fromQueryParameter(new ArrayList<String>(){{ add("yellow"); }}))
                .withSelection(new ArrayList<String>(){{ add("properties.@ns:com:here:xyz.tags");}})
                .withBbox(new BBox(-170, -170, 170, 170));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());
        assertEquals(1, new ObjectMapper().convertValue(features.get(0).getProperties(), Map.class).size());
        assertEquals(1, features.get(0).getProperties().getXyzNamespace().getTags().size());

        // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
        getFeaturesByBBoxEvent
                = new GetFeaturesByTileEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTags(TagsQuery.fromQueryParameter(new ArrayList<String>(){{ add("yellow"); }}))
                .withSelection(new ArrayList<String>(){{ add("properties");}})
                .withBbox(new BBox(-170, -170, 170, 170));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());
        assertEquals(2, new ObjectMapper().convertValue(features.get(0).getProperties(), Map.class).size());

        // =========== QUERY BBOX - +SMALL; +TAGS ==========
        getFeaturesByBBoxEvent
                = new GetFeaturesByTileEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTags(TagsQuery.fromQueryParameter(new ArrayList<String>(){{ add("yellow"); }}))
                .withBbox(new BBox(10, -5, 20, 5));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());
    }

    @Test
    public void testGetFeaturesByGeometryQuery() throws Exception {
        XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
        FeatureCollection collection = new FeatureCollection();
        List<Feature> featureList = new ArrayList<>();
        // =========== INSERT Point Grid 20x20 ==========
        for (double x = 7.; x < 7.19d; x += 0.01d) {
            for (double y = 50.; y < 50.19d; y += 0.01d) {
                Feature f = new Feature()
                        .withGeometry(
                                new Point().withCoordinates(new PointCoordinates((Math.round(x * 10000.0) / 10000.0), Math.round(y * 10000.0) / 10000.0)))
                        .withProperties(new Properties().with("foo", Math.round(x * 10000.0) / 10000.0).with("foo2", 1).withXyzNamespace(xyzNamespace));
                featureList.add(f);
            }
        }
        // =========== INSERT Polygon  ==========
        PolygonCoordinates singlePoly = new PolygonCoordinates();
        LinearRingCoordinates rC = new LinearRingCoordinates();
        rC.add(new Position(7.01, 50.01));
        rC.add(new Position(7.03, 50.01));
        rC.add(new Position(7.03, 50.03));
        rC.add(new Position(7.01, 50.03));
        rC.add(new Position(7.01, 50.01));
        singlePoly.add(rC);

        Feature f = new Feature()
                .withGeometry(
                        new Polygon().withCoordinates(singlePoly))
                .withProperties(new Properties().with("foo", 999.1).withXyzNamespace(xyzNamespace));
        featureList.add(f);

        collection.setFeatures(featureList);
        // =========== INSERT Polygon  inside hole ==========
        singlePoly = new PolygonCoordinates();
        rC = new LinearRingCoordinates();
        rC.add(new Position(7.06, 50.07));
        rC.add(new Position(7.08, 50.07));
        rC.add(new Position(7.08, 50.08));
        rC.add(new Position(7.06, 50.08));
        rC.add(new Position(7.06, 50.07));
        singlePoly.add(rC);

        f = new Feature()
                .withGeometry(
                        new Polygon().withCoordinates(singlePoly))
                .withProperties(new Properties().with("foo", 999.2).withXyzNamespace(xyzNamespace));
        featureList.add(f);
        collection.setFeatures(featureList);

        // =========== INSERT Line ==========
        LineStringCoordinates lcCoords = new LineStringCoordinates();
        lcCoords.add(new Position(7.02, 50.02));
        lcCoords.add(new Position(7.18, 50.18));

        f = new Feature()
                .withGeometry(
                        new LineString().withCoordinates(lcCoords))
                .withProperties(new Properties().with("foo", 999.3).withXyzNamespace(xyzNamespace));
        featureList.add(f);

        // =========== INSERT Line ==========
        lcCoords = new LineStringCoordinates();
        lcCoords.add(new Position(7.16, 50.01));
        lcCoords.add(new Position(7.19, 50.01));

        f = new Feature()
                .withGeometry(
                        new LineString().withCoordinates(lcCoords))
                .withProperties(new Properties().with("foo", 999.4).withXyzNamespace(xyzNamespace));
        featureList.add(f);

        collection.setFeatures(featureList);

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withEnableUUID(true)
                .withInsertFeatures(collection.getFeatures());

        invokeLambda(mfevent.serialize());
        logger.info("Insert feature tested successfully");
        // =========== QUERY WITH POLYGON ==========
        PolygonCoordinates polyCoords = new PolygonCoordinates();
        LinearRingCoordinates ringCords = new LinearRingCoordinates();
        ringCords.add(new Position(7, 50));
        ringCords.add(new Position(7, 50.1));
        ringCords.add(new Position(7.1, 50.1));
        ringCords.add(new Position(7.1, 50));
        ringCords.add(new Position(7, 50));
        polyCoords.add(ringCords);

        Geometry geo = new Polygon().withCoordinates(polyCoords);

        GetFeaturesByGeometryEvent geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withGeometry(geo);

        String queryResponse = invokeLambda(geometryEvent.serialize());
        FeatureCollection featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(124, featureCollection.getFeatures().size());
        logger.info("Area Query with POLYGON tested successfully");
        // =========== QUERY WITH POLYGON WITH HOLE ==========
        LinearRingCoordinates holeCords = new LinearRingCoordinates();
        holeCords.add(new Position(7.05, 50.05));
        holeCords.add(new Position(7.05, 50.09));
        holeCords.add(new Position(7.09, 50.09));
        holeCords.add(new Position(7.09, 50.05));
        holeCords.add(new Position(7.05, 50.05));
        polyCoords.add(holeCords);

        geo = new Polygon().withCoordinates(polyCoords);
        geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withGeometry(geo);

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        for (Feature feature : featureCollection.getFeatures()) {
            /* try to find polygon inside the hole  */
            if ((feature.getProperties().get("foo")).toString().contains(("999.2"))) {
                fail();
            }
        }
        assertEquals(114, featureCollection.getFeatures().size());
        logger.info("Area Query with POLYGON incl. hole tested successfully");
        // =========== QUERY WITH MULTIPOLYGON ==========
        PolygonCoordinates polyCoords2 = new PolygonCoordinates();
        LinearRingCoordinates ringCords2 = new LinearRingCoordinates();
        ringCords2.add(new Position(7.1, 50.1));
        ringCords2.add(new Position(7.2, 50.1));
        ringCords2.add(new Position(7.2, 50.2));
        ringCords2.add(new Position(7.1, 50.2));
        ringCords2.add(new Position(7.1, 50.1));
        polyCoords2.add(ringCords2);

        MultiPolygonCoordinates multiCords = new MultiPolygonCoordinates();
        multiCords.add(polyCoords);
        multiCords.add(polyCoords2);

        geo = new MultiPolygon().withCoordinates(multiCords);
        geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withGeometry(geo);

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        int cnt = 0;
        for (Feature feature : featureCollection.getFeatures()) {
            /* Try to find the both polygons */
            if ((feature.getProperties().get("foo")).toString().contains(("999."))) {
                cnt++;
            }
        }
        assertEquals(213, featureCollection.getFeatures().size());
        assertEquals(2, cnt);
        logger.info("Area Query with MULTIPOLYGON tested successfully");
        // =========== QUERY WITH MULTIPOLYGON + PROPERTIES_SEARCH ==========
        PropertiesQuery pq = new PropertiesQuery();
        PropertyQueryList pql = new PropertyQueryList();
        pql.add(new PropertyQuery().withKey("properties.foo").withOperation(PropertyQuery.QueryOperation.LESS_THAN_OR_EQUALS)
                .withValues(new ArrayList<>(Collections.singletonList(7.1))));
        pq.add(pql);

        geo = new MultiPolygon().withCoordinates(multiCords);
        geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withGeometry(geo)
                .withPropertiesQuery(pq);

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(121, featureCollection.getFeatures().size());
        logger.info("Area Query with MULTIPOLYGON + PROPERTIES_SEARCH tested successfully");
        // =========== QUERY WITH MULTIPOLYGON + SELECTION ==========
        geo = new MultiPolygon().withCoordinates(multiCords);
        geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withGeometry(geo)
                .withSelection(new ArrayList<>(Collections.singletonList("properties.foo2")));

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(213, featureCollection.getFeatures().size());

        Properties properties = featureCollection.getFeatures().get(0).getProperties();
        assertEquals(new Integer(1), properties.get("foo2"));
        assertNull(properties.get("foo"));
        logger.info("Area Query with MULTIPOLYGON + SELECTION tested successfully");
    }

    @Test
    public void testGetStatisticsEvent() throws Exception {

        // =========== INSERT ==========
        XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
        String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        logger.info("Insert feature tested successfully");

        // =========== GetStatistics ==========
        GetStatisticsEvent event = new GetStatisticsEvent()
            .withConnectorParams(defaultTestConnectorParams)
            .withSpace("foo");

        String eventJson = event.serialize();
        String statisticsJson = invokeLambda(eventJson);
        StatisticsResponse response = XyzSerializable.deserialize(statisticsJson);

        assertNotNull(response);

        assertEquals(new Long(3), response.getCount().getValue());
        assertEquals(false, response.getCount().getEstimated());

        assertTrue(response.getByteSize().getValue() > 0);
        assertEquals(true, response.getByteSize().getEstimated());

        assertNotNull(response.getBbox());
        assertEquals(false, response.getBbox().getEstimated());

        assertEquals("name", response.getProperties().getValue().get(0).getKey());
        assertEquals("string", response.getProperties().getValue().get(0).getDatatype());
        assertEquals(3, response.getProperties().getValue().get(0).getCount());
        assertEquals(false, response.getProperties().getEstimated());
        assertEquals(StatisticsResponse.PropertiesStatistics.Searchable.ALL, response.getProperties().getSearchable());

        assertEquals(3, response.getTags().getValue().size());
        assertEquals(false, response.getTags().getEstimated());

        assertEquals(new ArrayList<>(Collections.singletonList("Point")), response.getGeometryTypes().getValue());
        assertEquals(false, response.getGeometryTypes().getEstimated());

        // =========== INSERT 11k ==========
        FeatureCollection collection = new FeatureCollection();

        List<String> pKeys = Stream.generate(() ->
                RandomStringUtils.randomAlphanumeric(10)).limit(3).collect(Collectors.toList());

        collection.setFeatures(new ArrayList<>());
        collection.getFeatures().addAll(
                Stream.generate(() -> {
                    Feature f = new Feature()
                            .withGeometry(
                                    new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
                            .withProperties(new Properties().withXyzNamespace(xyzNamespace));
                    pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(8)));
                    return f;
                }).limit(11000).collect(Collectors.toList()));

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withEnableUUID(true)
                .withInsertFeatures(collection.getFeatures()); // TODO use get11kFeatureCollection() and extract pKeys

        invokeLambda(mfevent.serialize());

        /* Needed to trigger update on pg_stat*/
        try (final Connection connection = lambda.dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("ANALYZE \"foo\";");
        }

        statisticsJson = invokeLambda(eventJson);
        // =========== GetStatistics ==========
        response = XyzSerializable.deserialize(statisticsJson);

        assertEquals(new Long(11003), response.getCount().getValue());

        assertEquals(true, response.getCount().getEstimated());
        assertEquals(true, response.getByteSize().getEstimated());
        assertEquals(true, response.getBbox().getEstimated());
        assertEquals(true, response.getTags().getEstimated());
        assertEquals(true, response.getGeometryTypes().getEstimated());

        assertEquals(StatisticsResponse.PropertiesStatistics.Searchable.PARTIAL, response.getProperties().getSearchable());

        for (StatisticsResponse.PropertyStatistics prop : response.getProperties().getValue()) {
            if(prop.getKey().equalsIgnoreCase("name"))
                continue;
            assertTrue(pKeys.contains(prop.getKey()));
            assertEquals(prop.getCount() > 10000, true);
        }
    }

    @Test
    public void testSearchAndCountByPropertiesAndTags() throws Exception {
        // prepare the data
        TypeReference<Map<String, Object>> tr = new TypeReference<Map<String, Object>>() {
        };
        ObjectMapper mapper = new ObjectMapper();

        String insertJsonFile = "/events/InsertFeaturesForSearchTestEvent.json";
        invokeLambdaFromFile(insertJsonFile);

        // retrieve the basic event
        String basic = IOUtils.toString(GSContext.class.getResourceAsStream("/events/BasicSearchByPropertiesAndTagsEvent.json"));

        // Test 1
        Map<String, Object> test1 = mapper.readValue(basic, tr);
        Map<String, Object> properties1 = new HashMap<>();
        properties1.put("key", "properties.name");
        properties1.put("operation", "EQUALS");
        properties1.put("values", Stream.of("Toyota").collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test1, properties1);
        addTagsToSearchObject(test1, "yellow");
        invokeAndAssert(test1, 1, "Toyota");

        // Test 2
        Map<String, Object> test2 = mapper.readValue(basic, tr);
        Map<String, Object> properties2 = new HashMap<>();
        properties2.put("key", "properties.size");
        properties2.put("operation", "LESS_THAN_OR_EQUALS");
        properties2.put("values", Stream.of(1).collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test2, properties2);
        invokeAndAssert(test2, 2, "Ducati", "BikeX");

        // Test 3
        Map<String, Object> test3 = mapper.readValue(basic, tr);
        Map<String, Object> properties3 = new HashMap<>();
        properties3.put("key", "properties.car");
        properties3.put("operation", "EQUALS");
        properties3.put("values", Stream.of(true).collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test3, properties3);
        invokeAndAssert(test3, 1, "Toyota");

        // Test 4
        Map<String, Object> test4 = mapper.readValue(basic, tr);
        Map<String, Object> properties4 = new HashMap<>();
        properties4.put("key", "properties.car");
        properties4.put("operation", "EQUALS");
        properties4.put("values", Stream.of(false).collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test4, properties4);
        invokeAndAssert(test4, 1, "Ducati");

        // Test 5
        Map<String, Object> test5 = mapper.readValue(basic, tr);
        Map<String, Object> properties5 = new HashMap<>();
        properties5.put("key", "properties.size");
        properties5.put("operation", "GREATER_THAN");
        properties5.put("values", Stream.of(5).collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test5, properties5);
        addTagsToSearchObject(test5, "red");
        invokeAndAssert(test5, 1, "Toyota");

        // Test 6
        Map<String, Object> test6 = mapper.readValue(basic, tr);
        Map<String, Object> properties6 = new HashMap<>();
        properties6.put("key", "properties.size");
        properties6.put("operation", "LESS_THAN");
        properties6.put("values", Stream.of(5).collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test6, properties6);
        addTagsToSearchObject(test6, "red");
        invokeAndAssert(test6, 1, "Ducati");

        // Test 7
        Map<String, Object> test7 = mapper.readValue(basic, tr);
        Map<String, Object> properties7 = new HashMap<>();
        properties7.put("key", "properties.name");
        properties7.put("operation", "EQUALS");
        properties7.put("values", Stream.of("Toyota", "Tesla").collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test7, properties7);
        invokeAndAssert(test7, 2, "Toyota", "Tesla");

        // Test 8
        Map<String, Object> test8 = mapper.readValue(basic, tr);
        invokeAndAssert(test8, 4, "Toyota", "Tesla", "Ducati", "BikeX");

        // Test 9
        Map<String, Object> test9 = mapper.readValue(basic, tr);
        Map<String, Object> properties9 = new HashMap<>();
        properties9.put("key", "properties.name");
        properties9.put("operation", "EQUALS");
        properties9.put("values", Stream.of("Test").collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test9, properties9);
        invokeAndAssert(test9, 0);

        // Test 10
        Map<String, Object> test10 = mapper.readValue(basic, tr);
        Map<String, Object> properties10 = new HashMap<>();
        properties10.put("key", "properties.name");
        properties10.put("operation", "EQUALS");
        properties10.put("values", Stream.of("Toyota").collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test10, properties10);
        addTagsToSearchObject(test10, "cyan");
        invokeAndAssert(test10, 0);

        // Test 11
        Map<String, Object> test11 = mapper.readValue(basic, tr);
        Map<String, Object> properties11_1 = new HashMap<>();
        properties11_1.put("key", "properties.name");
        properties11_1.put("operation", "EQUALS");
        properties11_1.put("values", Stream.of("Toyota", "Ducati", "BikeX").collect(Collectors.toList()));
        Map<String, Object> properties11_2 = new HashMap<>();
        properties11_2.put("key", "properties.size");
        properties11_2.put("operation", "EQUALS");
        properties11_2.put("values", Stream.of(1D, 0.3D).collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test11, properties11_1, properties11_2);
        invokeAndAssert(test11, 2, "Ducati", "BikeX");

        // Test 12
        Map<String, Object> test12 = mapper.readValue(basic, tr);
        Map<String, Object> properties12_1 = new HashMap<>();
        properties12_1.put("key", "properties.name");
        properties12_1.put("operation", "EQUALS");
        properties12_1.put("values", Stream.of("Toyota", "Ducati").collect(Collectors.toList()));
        Map<String, Object> properties12_2 = new HashMap<>();
        properties12_2.put("key", "properties.name");
        properties12_2.put("operation", "EQUALS");
        properties12_2.put("values", Stream.of("Toyota").collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test12, properties12_1);
        addPropertiesQueryToSearchObject(test12, true, properties12_2);
        invokeAndAssert(test12, 2, "Toyota", "Ducati");

        // Test 13
        Map<String, Object> test13 = mapper.readValue(basic, tr);
        Map<String, Object> properties13_1 = new HashMap<>();
        properties13_1.put("key", "properties.name");
        properties13_1.put("operation", "EQUALS");
        properties13_1.put("values", Stream.of("Toyota").collect(Collectors.toList()));
        Map<String, Object> properties13_2 = new HashMap<>();
        properties13_2.put("key", "properties.name");
        properties13_2.put("operation", "EQUALS");
        properties13_2.put("values", Stream.of("Ducati").collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test13, properties13_1);
        addPropertiesQueryToSearchObject(test13, true, properties13_2);
        invokeAndAssert(test13, 2, "Toyota", "Ducati");

        // Test 14
        Map<String, Object> test14 = mapper.readValue(basic, tr);
        Map<String, Object> properties14_1 = new HashMap<>();
        properties14_1.put("key", "id");
        properties14_1.put("operation", "GREATER_THAN");
        properties14_1.put("values", Stream.of(0).collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test14, properties14_1);

        String response = invokeLambda(mapper.writeValueAsString(test14));
        FeatureCollection responseCollection = XyzSerializable.deserialize(response);
        List<Feature> responseFeatures = responseCollection.getFeatures();
        String id = responseFeatures.get(0).getId();
        assertEquals("Check size", 4, responseFeatures.size());

        Map<String, Object> test15 = mapper.readValue(basic, tr);
        Map<String, Object> properties15_1 = new HashMap<>();
        properties15_1.put("key", "id");
        properties15_1.put("operation", "EQUALS");
        properties15_1.put("values", Stream.of(id).collect(Collectors.toList()));
        addPropertiesQueryToSearchObject(test15, properties15_1);

        response = invokeLambda(mapper.writeValueAsString(test15));
        responseCollection = XyzSerializable.deserialize(response);
        responseFeatures = responseCollection.getFeatures();
        assertEquals(1, responseFeatures.size());

        // Test 15
        Map<String, Object> test16 = mapper.readValue(basic, tr);
        test16.put("type", "IterateFeaturesEvent");
        test16.put("handle", "1");
        invokeAndAssert(test16, 3, "Tesla", "Ducati", "BikeX");
    }

    @Test
    public void testIterate() throws Exception {
        final String response = invokeLambdaFromFile("/events/IterateMySpace.json");
        final FeatureCollection features = XyzSerializable.deserialize(response);
        features.serialize(true);
    }
}
