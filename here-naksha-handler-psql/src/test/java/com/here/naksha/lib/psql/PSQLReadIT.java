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
package com.here.naksha.lib.psql;

import static org.junit.jupiter.api.Assertions.*;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.naksha.lib.core.models.geojson.coordinates.*;
import com.here.naksha.lib.core.models.geojson.implementation.*;
import com.here.naksha.lib.core.models.geojson.implementation.Properties;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.events.PropertyQuery;
import com.here.naksha.lib.core.models.payload.events.PropertyQueryAnd;
import com.here.naksha.lib.core.models.payload.events.PropertyQueryOr;
import com.here.naksha.lib.core.models.payload.events.QueryOperation;
import com.here.naksha.lib.core.models.payload.events.TagsQuery;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByGeometryEvent;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileEvent;
import com.here.naksha.lib.core.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.info.GetStatisticsEvent;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.StatisticsResponse;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PSQLReadIT extends PSQLAbstractIT {

    @BeforeAll
    public static void init() throws Exception {
        initEnv(null);
    }

    @AfterAll
    public void shutdown() throws Exception {
        invokeDeleteTestSpace(null);
    }

    /** Test all branches of the BBox query. */
    @Test
    public void testBBoxQuery() throws Exception {
        // =========== INSERT ==========
        final String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
        final String insertResponse = invokeLambdaFromFile(insertJsonFile);
        final String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        LOGGER.info("Insert feature tested successfully");

        String queryEvent;
        // =========== QUERY BBOX ==========
        GetFeaturesByTileEvent getFeaturesByBBoxEvent = new GetFeaturesByTileEvent();
        // getFeaturesByBBoxEvent.setConnectorParams(defaultTestConnectorParams);
        // getFeaturesByBBoxEvent.setSpaceId("foo");
        getFeaturesByBBoxEvent.setBbox(new BBox(-170, -170, 170, 170));
        getFeaturesByBBoxEvent.setLimit(30000);

        String queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        FeatureCollection featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        List<Feature> features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(3, features.size());

        // =========== QUERY BBOX - +TAGS ==========
        getFeaturesByBBoxEvent = new GetFeaturesByTileEvent();
        // getFeaturesByBBoxEvent.setConnectorParams(defaultTestConnectorParams);
        // getFeaturesByBBoxEvent.setSpaceId("foo");
        getFeaturesByBBoxEvent.setTags(TagsQuery.fromQueryParameter(new ArrayList<String>() {
            {
                add("yellow");
            }
        }));
        getFeaturesByBBoxEvent.setBbox(new BBox(-170, -170, 170, 170));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());

        // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
        getFeaturesByBBoxEvent = new GetFeaturesByTileEvent();
        // getFeaturesByBBoxEvent.setConnectorParams(defaultTestConnectorParams);
        // getFeaturesByBBoxEvent.setSpaceId("foo");
        getFeaturesByBBoxEvent.setTags(TagsQuery.fromQueryParameter(new ArrayList<String>() {
            {
                add("yellow");
            }
        }));
        getFeaturesByBBoxEvent.setSelection(new ArrayList<String>() {
            {
                add("id");
                add("type");
                add("geometry");
                add("properties.name");
            }
        });
        getFeaturesByBBoxEvent.setBbox(new BBox(-170, -170, 170, 170));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        XyzResponse response = JsonSerializable.deserialize(queryResponse);
        if (response instanceof ErrorResponse) {
            failWithError((ErrorResponse) response);
        }
        featureCollection = (FeatureCollection) response;
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());
        assertEquals(
                1,
                new ObjectMapper()
                        .convertValue(features.get(0).getProperties(), Map.class)
                        .size());
        assertEquals("Toyota", features.get(0).getProperties().get("name"));
        assertNotNull(features.get(0).getId());
        assertNotNull(features.get(0));
        assertNotNull(features.get(0).getGeometry());

        // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
        getFeaturesByBBoxEvent = new GetFeaturesByTileEvent();
        // getFeaturesByBBoxEvent.setConnectorParams(defaultTestConnectorParams);
        // getFeaturesByBBoxEvent.setSpaceId("foo");
        getFeaturesByBBoxEvent.setTags(TagsQuery.fromQueryParameter(new ArrayList<String>() {
            {
                add("yellow");
            }
        }));
        getFeaturesByBBoxEvent.setSelection(new ArrayList<String>() {
            {
                add("properties.@ns:com:here:xyz.tags");
            }
        });
        getFeaturesByBBoxEvent.setBbox(new BBox(-170, -170, 170, 170));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());
        assertEquals(
                1,
                new ObjectMapper()
                        .convertValue(features.get(0).getProperties(), Map.class)
                        .size());
        assertEquals(
                1, features.get(0).getProperties().getXyzNamespace().getTags().size());

        // =========== QUERY WITH SELECTION BBOX - +TAGS ==========
        getFeaturesByBBoxEvent = new GetFeaturesByTileEvent();
        // getFeaturesByBBoxEvent.setConnectorParams(defaultTestConnectorParams);
        // getFeaturesByBBoxEvent.setSpaceId("foo");
        getFeaturesByBBoxEvent.setTags(TagsQuery.fromQueryParameter(new ArrayList<String>() {
            {
                add("yellow");
            }
        }));
        getFeaturesByBBoxEvent.setSelection(new ArrayList<String>() {
            {
                add("properties");
            }
        });
        getFeaturesByBBoxEvent.setBbox(new BBox(-170, -170, 170, 170));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        features = featureCollection.getFeatures();
        assertNotNull(features);
        assertEquals(1, features.size());
        assertEquals(
                2,
                new ObjectMapper()
                        .convertValue(features.get(0).getProperties(), Map.class)
                        .size());

        // =========== QUERY BBOX - +SMALL; +TAGS ==========
        getFeaturesByBBoxEvent = new GetFeaturesByTileEvent();
        // getFeaturesByBBoxEvent.setConnectorParams(defaultTestConnectorParams);
        // getFeaturesByBBoxEvent.setSpaceId("foo");
        getFeaturesByBBoxEvent.setTags(TagsQuery.fromQueryParameter(new ArrayList<String>() {
            {
                add("yellow");
            }
        }));
        getFeaturesByBBoxEvent.setBbox(new BBox(10, -5, 20, 5));

        queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        featureCollection = JsonSerializable.deserialize(queryResponse);
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
                Feature f = new Feature(RandomStringUtils.randomAlphabetic(12));
                f.setGeometry(new Point()
                        .withCoordinates(new PointCoordinates(
                                (Math.round(x * 10000.0) / 10000.0), Math.round(y * 10000.0) / 10000.0)));
                f.getProperties().put("foo", Math.round(x * 10000.0) / 10000.0);
                f.getProperties().put("foo2", 1);
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

        Feature f = new Feature(RandomStringUtils.randomAlphabetic(8));
        f.setGeometry(new Polygon().withCoordinates(singlePoly));
        f.getProperties().put("foo", 999.1);
        f.getProperties().setXyzNamespace(xyzNamespace);
        featureList.add(f);

        collection.setLazyParsableFeatureList(featureList);
        // =========== INSERT Polygon  inside hole ==========
        singlePoly = new PolygonCoordinates();
        rC = new LinearRingCoordinates();
        rC.add(new Position(7.06, 50.07));
        rC.add(new Position(7.08, 50.07));
        rC.add(new Position(7.08, 50.08));
        rC.add(new Position(7.06, 50.08));
        rC.add(new Position(7.06, 50.07));
        singlePoly.add(rC);

        f = new Feature(RandomStringUtils.randomAlphabetic(8));
        f.setGeometry(new Polygon().withCoordinates(singlePoly));
        f.getProperties().put("foo", 999.2);
        f.getProperties().setXyzNamespace(xyzNamespace);
        featureList.add(f);
        collection.setLazyParsableFeatureList(featureList);

        // =========== INSERT Line ==========
        LineStringCoordinates lcCoords = new LineStringCoordinates();
        lcCoords.add(new Position(7.02, 50.02));
        lcCoords.add(new Position(7.18, 50.18));

        f = new Feature(RandomStringUtils.randomAlphabetic(8));
        f.setGeometry(new LineString().withCoordinates(lcCoords));
        f.getProperties().put("foo", 999.3);
        f.getProperties().setXyzNamespace(xyzNamespace);
        featureList.add(f);

        // =========== INSERT Line ==========
        lcCoords = new LineStringCoordinates();
        lcCoords.add(new Position(7.16, 50.01));
        lcCoords.add(new Position(7.19, 50.01));

        f = new Feature(RandomStringUtils.randomAlphabetic(8));
        f.setGeometry(new LineString().withCoordinates(lcCoords));
        f.getProperties().put("foo", 999.4);
        f.getProperties().setXyzNamespace(xyzNamespace);
        featureList.add(f);

        collection.setLazyParsableFeatureList(featureList);

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
        // mfevent.setConnectorParams(defaultTestConnectorParams);
        // mfevent.setSpaceId("foo");
        mfevent.setTransaction(true);
        mfevent.setEnableUUID(true);
        mfevent.setInsertFeatures(collection.getFeatures());

        invokeLambda(mfevent.serialize());
        LOGGER.info("Insert feature tested successfully");
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

        GetFeaturesByGeometryEvent geometryEvent = new GetFeaturesByGeometryEvent();
        // geometryEvent.setConnectorParams(defaultTestConnectorParams);
        // geometryEvent.setSpaceId("foo");
        geometryEvent.setGeometry(geo);

        String queryResponse = invokeLambda(geometryEvent.serialize());
        FeatureCollection featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(124, featureCollection.getFeatures().size());
        LOGGER.info("Area Query with POLYGON tested successfully");
        // =========== QUERY WITH POLYGON WITH HOLE ==========
        LinearRingCoordinates holeCords = new LinearRingCoordinates();
        holeCords.add(new Position(7.05, 50.05));
        holeCords.add(new Position(7.05, 50.09));
        holeCords.add(new Position(7.09, 50.09));
        holeCords.add(new Position(7.09, 50.05));
        holeCords.add(new Position(7.05, 50.05));
        polyCoords.add(holeCords);

        geo = new Polygon().withCoordinates(polyCoords);
        geometryEvent = new GetFeaturesByGeometryEvent();
        // geometryEvent.setConnectorParams(defaultTestConnectorParams);
        // geometryEvent.setSpaceId("foo");
        geometryEvent.setGeometry(geo);

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        for (Feature feature : featureCollection.getFeatures()) {
            /* try to find polygon inside the hole  */
            if ((feature.getProperties().get("foo")).toString().contains(("999.2"))) {
                fail();
            }
        }
        assertEquals(114, featureCollection.getFeatures().size());
        LOGGER.info("Area Query with POLYGON incl. hole tested successfully");
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
        geometryEvent = new GetFeaturesByGeometryEvent();
        // geometryEvent.setConnectorParams(defaultTestConnectorParams);
        // geometryEvent.setSpaceId("foo");
        geometryEvent.setGeometry(geo);

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = JsonSerializable.deserialize(queryResponse);
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
        LOGGER.info("Area Query with MULTIPOLYGON tested successfully");
        // =========== QUERY WITH MULTIPOLYGON + PROPERTIES_SEARCH ==========
        PropertyQueryOr pq = new PropertyQueryOr();
        PropertyQueryAnd pql = new PropertyQueryAnd();
        pql.add(new PropertyQuery()
                .withKey("properties.foo")
                .withOperation(QueryOperation.LESS_THAN_OR_EQUALS)
                .withValues(new ArrayList<>(Collections.singletonList(7.1))));
        pq.add(pql);

        geo = new MultiPolygon().withCoordinates(multiCords);
        geometryEvent = new GetFeaturesByGeometryEvent();
        // geometryEvent.setConnectorParams(defaultTestConnectorParams);
        // geometryEvent.setSpaceId("foo");
        geometryEvent.setGeometry(geo);
        geometryEvent.setPropertiesQuery(pq);

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(121, featureCollection.getFeatures().size());
        LOGGER.info("Area Query with MULTIPOLYGON + PROPERTIES_SEARCH tested successfully");
        // =========== QUERY WITH MULTIPOLYGON + SELECTION ==========
        geo = new MultiPolygon().withCoordinates(multiCords);
        geometryEvent = new GetFeaturesByGeometryEvent();
        // geometryEvent.setConnectorParams(defaultTestConnectorParams);
        // geometryEvent.setSpaceId("foo");
        geometryEvent.setGeometry(geo);
        geometryEvent.setSelection(new ArrayList<>(Collections.singletonList("properties.foo2")));

        queryResponse = invokeLambda(geometryEvent.serialize());
        XyzResponse response = JsonSerializable.deserialize(queryResponse);
        if (response instanceof ErrorResponse) {
            failWithError((ErrorResponse) response);
        }
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(213, featureCollection.getFeatures().size());

        Properties properties = featureCollection.getFeatures().get(0).getProperties();
        assertEquals(Integer.valueOf(1), properties.get("foo2"));
        assertNull(properties.get("foo"));
        LOGGER.info("Area Query with MULTIPOLYGON + SELECTION tested successfully");

        // =========== QUERY WITH H3Index ==========
        geometryEvent = new GetFeaturesByGeometryEvent();
        // geometryEvent.setConnectorParams(defaultTestConnectorParams);
        // geometryEvent.setSpaceId("foo");
        // Large one
        geometryEvent.setH3Index("821fa7fffffffff");

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(404, featureCollection.getFeatures().size());
        LOGGER.info("Hexbin Query (large) tested successfully");

        geometryEvent = new GetFeaturesByGeometryEvent();
        // geometryEvent.setConnectorParams(defaultTestConnectorParams);
        // geometryEvent.setSpaceId("foo");
        // Small one
        geometryEvent.setH3Index("861fa3a07ffffff");

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(42, featureCollection.getFeatures().size());
        LOGGER.info("H3Index Query (small) tested successfully");

        pq = new PropertyQueryOr();
        pql = new PropertyQueryAnd();
        pql.add(new PropertyQuery()
                .withKey("geometry.type")
                .withOperation(QueryOperation.EQUALS)
                .withValues(new ArrayList<>(Collections.singletonList("Polygon"))));
        pq.add(pql);

        geometryEvent = new GetFeaturesByGeometryEvent();
        // geometryEvent.setConnectorParams(defaultTestConnectorParams);
        // geometryEvent.setSpaceId("foo");
        geometryEvent.setPropertiesQuery(pq);
        // Small one
        geometryEvent.setH3Index("861fa3a07ffffff");

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = JsonSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(1, featureCollection.getFeatures().size());
        LOGGER.info("H3Index Query (small) with property query tested successfully");
    }

    @Test
    public void testGetStatisticsEvent() throws Exception {

        // =========== INSERT ==========
        XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
        String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        LOGGER.info("Insert feature tested successfully");

        // =========== GetStatistics ==========
        GetStatisticsEvent event = new GetStatisticsEvent();
        // event.setConnectorParams(defaultTestConnectorParams);
        // event.setSpaceId("foo");

        String eventJson = event.serialize();
        String statisticsJson = invokeLambda(eventJson);
        StatisticsResponse response = JsonSerializable.deserialize(statisticsJson);

        assertNotNull(response);

        assertEquals(Long.valueOf(3L), response.getCount().getValue());
        assertEquals(false, response.getCount().getEstimated());

        assertTrue(response.getByteSize().getValue() > 0);
        assertEquals(true, response.getByteSize().getEstimated());

        assertNotNull(response.getBbox());
        assertEquals(false, response.getBbox().getEstimated());

        assertEquals("name", response.getProperties().getValue().get(0).getKey());
        assertEquals("string", response.getProperties().getValue().get(0).getDatatype());
        assertEquals(3, response.getProperties().getValue().get(0).getCount());
        assertEquals(false, response.getProperties().getEstimated());
        assertEquals(
                StatisticsResponse.PropertiesStatistics.Searchable.ALL,
                response.getProperties().getSearchable());

        assertEquals(3, response.getTags().getValue().size());
        assertEquals(false, response.getTags().getEstimated());

        assertEquals(
                new ArrayList<>(Collections.singletonList("Point")),
                response.getGeometryTypes().getValue());
        assertEquals(false, response.getGeometryTypes().getEstimated());

        // =========== INSERT 11k ==========
        FeatureCollection collection = new FeatureCollection();

        List<String> pKeys = Stream.generate(() -> RandomStringUtils.randomAlphanumeric(10))
                .limit(3)
                .collect(Collectors.toList());

        collection.setFeatures(new ArrayList<>());
        collection
                .getFeatures()
                .addAll(Stream.generate(() -> {
                            Feature f = new Feature(RandomStringUtils.randomAlphabetic(8));
                            f.setGeometry(new Point()
                                    .withCoordinates(new PointCoordinates(
                                            360d * RANDOM.nextDouble() - 180d, 180d * RANDOM.nextDouble() - 90d)));
                            f.getProperties().setXyzNamespace(xyzNamespace);
                            pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(8)));
                            return f;
                        })
                        .limit(11000)
                        .collect(Collectors.toList()));

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
        // mfevent.setConnectorParams(defaultTestConnectorParams);
        // mfevent.setSpaceId("foo");
        mfevent.setTransaction(true);
        mfevent.setEnableUUID(true);
        mfevent.setInsertFeatures(collection.getFeatures()); // TODO use get11kFeatureCollection() and extract pKeys

        invokeLambda(mfevent.serialize());

        /* Needed to trigger update on pg_stat*/
        try (final Connection connection = dataSource().getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("ANALYZE \"foo\";");
        }

        statisticsJson = invokeLambda(eventJson);
        // =========== GetStatistics ==========
        response = JsonSerializable.deserialize(statisticsJson);

        assertEquals(Long.valueOf(11003L), response.getCount().getValue());

        assertEquals(true, response.getCount().getEstimated());
        assertEquals(true, response.getByteSize().getEstimated());
        assertEquals(true, response.getBbox().getEstimated());
        assertEquals(true, response.getTags().getEstimated());
        assertEquals(true, response.getGeometryTypes().getEstimated());

        assertEquals(
                StatisticsResponse.PropertiesStatistics.Searchable.PARTIAL,
                response.getProperties().getSearchable());

        for (StatisticsResponse.PropertyStatistics prop :
                response.getProperties().getValue()) {
            if (prop.getKey().equalsIgnoreCase("name")) {
                continue;
            }
            assertTrue(pKeys.contains(prop.getKey()));
            assertEquals(prop.getCount() > 10000, true);
        }
    }

    @Test
    public void testSearchAndCountByPropertiesAndTags() throws Exception {
        // prepare the data
        TypeReference<Map<String, Object>> tr = new TypeReference<Map<String, Object>>() {};
        ObjectMapper mapper = new ObjectMapper();

        String insertJsonFile = "/events/InsertFeaturesForSearchTestEvent.json";
        invokeLambdaFromFile(insertJsonFile);

        // retrieve the basic event
        String basic = IOUtils.toString(
                this.getClass().getResourceAsStream("/events/BasicSearchByPropertiesAndTagsEvent.json"));

        // Test 1
        Map<String, Object> test1 = mapper.readValue(basic, tr);
        Map<String, Object> properties1 = new HashMap<>();
        properties1.put("key", "properties.name");
        properties1.put("operation", "EQUALS");
        properties1.put("values", Collections.singletonList("Toyota"));
        addPropertiesQueryToSearchObject(test1, false, properties1);
        addTagsToSearchObject(test1, "yellow");
        invokeAndAssert(test1, 1, "Toyota");

        // Test 2
        Map<String, Object> test2 = mapper.readValue(basic, tr);
        Map<String, Object> properties2 = new HashMap<>();
        properties2.put("key", "properties.size");
        properties2.put("operation", "LESS_THAN_OR_EQUALS");
        properties2.put("values", Collections.singletonList(1));
        addPropertiesQueryToSearchObject(test2, false, properties2);
        invokeAndAssert(test2, 2, "Ducati", "BikeX");

        // Test 3
        Map<String, Object> test3 = mapper.readValue(basic, tr);
        Map<String, Object> properties3 = new HashMap<>();
        properties3.put("key", "properties.car");
        properties3.put("operation", "EQUALS");
        properties3.put("values", Collections.singletonList(true));
        addPropertiesQueryToSearchObject(test3, false, properties3);
        invokeAndAssert(test3, 1, "Toyota");

        // Test 4
        Map<String, Object> test4 = mapper.readValue(basic, tr);
        Map<String, Object> properties4 = new HashMap<>();
        properties4.put("key", "properties.car");
        properties4.put("operation", "EQUALS");
        properties4.put("values", Collections.singletonList(false));
        addPropertiesQueryToSearchObject(test4, false, properties4);
        invokeAndAssert(test4, 1, "Ducati");

        // Test 5
        Map<String, Object> test5 = mapper.readValue(basic, tr);
        Map<String, Object> properties5 = new HashMap<>();
        properties5.put("key", "properties.size");
        properties5.put("operation", "GREATER_THAN");
        properties5.put("values", Collections.singletonList(5));
        addPropertiesQueryToSearchObject(test5, false, properties5);
        addTagsToSearchObject(test5, "red");
        invokeAndAssert(test5, 1, "Toyota");

        // Test 6
        Map<String, Object> test6 = mapper.readValue(basic, tr);
        Map<String, Object> properties6 = new HashMap<>();
        properties6.put("key", "properties.size");
        properties6.put("operation", "LESS_THAN");
        properties6.put("values", Collections.singletonList(5));
        addPropertiesQueryToSearchObject(test6, false, properties6);
        addTagsToSearchObject(test6, "red");
        invokeAndAssert(test6, 1, "Ducati");

        // Test 7
        Map<String, Object> test7 = mapper.readValue(basic, tr);
        Map<String, Object> properties7 = new HashMap<>();
        properties7.put("key", "properties.name");
        properties7.put("operation", "EQUALS");
        properties7.put("values", Arrays.asList("Toyota", "Tesla"));
        addPropertiesQueryToSearchObject(test7, false, properties7);
        invokeAndAssert(test7, 2, "Toyota", "Tesla");

        // Test 8
        Map<String, Object> test8 = mapper.readValue(basic, tr);
        invokeAndAssert(test8, 4, "Toyota", "Tesla", "Ducati", "BikeX");

        // Test 9
        Map<String, Object> test9 = mapper.readValue(basic, tr);
        Map<String, Object> properties9 = new HashMap<>();
        properties9.put("key", "properties.name");
        properties9.put("operation", "EQUALS");
        properties9.put("values", Collections.singletonList("Test"));
        addPropertiesQueryToSearchObject(test9, false, properties9);
        invokeAndAssert(test9, 0);

        // Test 10
        Map<String, Object> test10 = mapper.readValue(basic, tr);
        Map<String, Object> properties10 = new HashMap<>();
        properties10.put("key", "properties.name");
        properties10.put("operation", "EQUALS");
        properties10.put("values", Collections.singletonList("Toyota"));
        addPropertiesQueryToSearchObject(test10, false, properties10);
        addTagsToSearchObject(test10, "cyan");
        invokeAndAssert(test10, 0);

        // Test 11
        Map<String, Object> test11 = mapper.readValue(basic, tr);
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

        // Test 12
        Map<String, Object> test12 = mapper.readValue(basic, tr);
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

        // Test 13
        Map<String, Object> test13 = mapper.readValue(basic, tr);
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

        // Test 14
        Map<String, Object> test14 = mapper.readValue(basic, tr);
        Map<String, Object> properties14_1 = new HashMap<>();
        properties14_1.put("key", "id");
        properties14_1.put("operation", "GREATER_THAN");
        properties14_1.put("values", Collections.singletonList(0));
        addPropertiesQueryToSearchObject(test14, false, properties14_1);

        String response = invokeLambda(mapper.writeValueAsString(test14));
        FeatureCollection responseCollection = JsonSerializable.deserialize(response);
        List<Feature> responseFeatures = responseCollection.getFeatures();
        String id = responseFeatures.get(0).getId();
        assertEquals(4, responseFeatures.size());

        Map<String, Object> test15 = mapper.readValue(basic, tr);
        Map<String, Object> properties15_1 = new HashMap<>();
        properties15_1.put("key", "id");
        properties15_1.put("operation", "EQUALS");
        properties15_1.put("values", Collections.singletonList(id));
        addPropertiesQueryToSearchObject(test15, false, properties15_1);
        addPropertiesQueryToSearchObject(test15, true, properties15_1);

        response = invokeLambda(mapper.writeValueAsString(test15));
        responseCollection = JsonSerializable.deserialize(response);
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
        final FeatureCollection features = JsonSerializable.deserialize(response);
        features.serialize(true);
    }

    private void failWithError(ErrorResponse error) {
        if (error instanceof ErrorResponse) {
            fail("Received error response: [" + error.getError() + "] " + error.getErrorMessage());
        } else {
            fail("Failing without a valid ErrorResponse was provided.");
        }
    }

    @SafeVarargs
    protected final void addPropertiesQueryToSearchObject(
            Map<String, Object> json, boolean or, Map<String, Object>... objects) {
        if (!json.containsKey("propertiesQuery")) {
            json.put("propertiesQuery", new ArrayList<List<Map<String, Object>>>());
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        final List<List<Map<String, Object>>> list = (List) json.get("propertiesQuery");
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

        final FeatureCollection responseCollection = JsonSerializable.deserialize(response);
        final List<Feature> responseFeatures = responseCollection.getFeatures();
        assertEquals(size, responseFeatures.size());

        for (int i = 0; i < size; i++) {
            assertEquals(names[i], responseFeatures.get(i).getProperties().get("name"));
        }
    }
}
