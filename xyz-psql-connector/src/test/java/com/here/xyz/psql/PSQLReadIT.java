/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.LineStringCoordinates;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.MultiPolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.LineString;
import com.here.xyz.models.geojson.implementation.MultiPolygon;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.XyzResponse;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PSQLReadIT extends PSQLAbstractIT {

    @Before
    public void createSpace() throws Exception {
        invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    }

    @After
    public void shutdown() throws Exception {
        invokeDeleteTestSpace(null);
    }

    /**
     * Test all branches of the BBox query.
     */
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
        GetFeaturesByTileEvent getFeaturesByBBoxEvent
                = new GetFeaturesByTileEvent()
                    .withConnectorParams(defaultTestConnectorParams)
                    .withSpace("foo")
                    .withBbox(new BBox(-170, -170, 170, 170))
                    .withLimit(30000);

        String queryResponse = invokeLambda(getFeaturesByBBoxEvent.serialize());
        assertNotNull(queryResponse);
        FeatureCollection featureCollection = deserializeResponse(queryResponse);
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
        XyzResponse response = deserializeResponse(queryResponse);
        featureCollection = (FeatureCollection) response;
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
                .withConflictDetectionEnabled(true)
                .withInsertFeatures(collection.getFeatures());

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

        GetFeaturesByGeometryEvent geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withGeometry(geo);

        String queryResponse = invokeLambda(geometryEvent.serialize());
        FeatureCollection featureCollection = XyzSerializable.deserialize(queryResponse);
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
        LOGGER.info("Area Query with MULTIPOLYGON tested successfully");
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
        LOGGER.info("Area Query with MULTIPOLYGON + PROPERTIES_SEARCH tested successfully");
        // =========== QUERY WITH MULTIPOLYGON + SELECTION ==========
        geo = new MultiPolygon().withCoordinates(multiCords);
        geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withGeometry(geo)
                .withSelection(new ArrayList<>(Collections.singletonList("properties.foo2")));

        queryResponse = invokeLambda(geometryEvent.serialize());
        XyzResponse response = deserializeResponse(queryResponse);
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(213, featureCollection.getFeatures().size());

        Properties properties = featureCollection.getFeatures().get(0).getProperties();
        assertEquals(Integer.valueOf(1), properties.get("foo2"));
        assertNull(properties.get("foo"));
        LOGGER.info("Area Query with MULTIPOLYGON + SELECTION tested successfully");

        // =========== QUERY WITH H3Index ==========
        geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                //Large one
                .withH3Index("821fa7fffffffff");

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(404, featureCollection.getFeatures().size());
        LOGGER.info("Hexbin Query (large) tested successfully");

        geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                //Small one
                .withH3Index("861fa3a07ffffff");

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = XyzSerializable.deserialize(queryResponse);
        assertNotNull(featureCollection);
        assertEquals(42, featureCollection.getFeatures().size());
        LOGGER.info("H3Index Query (small) tested successfully");

        pq = new PropertiesQuery();
        pql = new PropertyQueryList();
        pql.add(new PropertyQuery().withKey("geometry.type").withOperation(PropertyQuery.QueryOperation.EQUALS)
                .withValues(new ArrayList<>(Collections.singletonList("Polygon"))));
        pq.add(pql);

        geometryEvent = new GetFeaturesByGeometryEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withPropertiesQuery(pq)
                //Small one
                .withH3Index("861fa3a07ffffff");

        queryResponse = invokeLambda(geometryEvent.serialize());
        featureCollection = XyzSerializable.deserialize(queryResponse);
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
        GetStatisticsEvent event = new GetStatisticsEvent()
            .withConnectorParams(defaultTestConnectorParams)
            .withSpace("foo");

        String eventJson = event.serialize();
        String statisticsJson = invokeLambda(eventJson);
        StatisticsResponse response = XyzSerializable.deserialize(statisticsJson);

        assertNotNull(response);

        assertEquals(Long.valueOf(3), response.getCount().getValue());
        assertEquals(false, response.getCount().getEstimated());

        assertTrue(response.getDataSize().getValue() > 0);
        assertEquals(true, response.getDataSize().getEstimated());

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
                                    new Point().withCoordinates(new PointCoordinates(360d * RANDOM.nextDouble() - 180d, 180d * RANDOM.nextDouble() - 90d)))
                            .withProperties(new Properties().withXyzNamespace(xyzNamespace));
                    pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(8)));
                    return f;
                }).limit(11000).collect(Collectors.toList()));

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withConflictDetectionEnabled(true)
                .withInsertFeatures(collection.getFeatures()); // TODO use get11kFeatureCollection() and extract pKeys

        invokeLambda(mfevent.serialize());

        /* Needed to trigger update on pg_stat*/
        try (final Connection connection = LAMBDA.dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("ANALYZE \"foo\";");
        }

        statisticsJson = invokeLambda(eventJson);
        // =========== GetStatistics ==========
        response = XyzSerializable.deserialize(statisticsJson);

        assertEquals(Long.valueOf(11003), response.getCount().getValue());

        assertEquals(true, response.getCount().getEstimated());
        assertEquals(true, response.getDataSize().getEstimated());
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
    public void testIterate() throws Exception {
        final String response = invokeLambdaFromFile("/events/IterateMySpace.json");
        final FeatureCollection features = XyzSerializable.deserialize(response);
        features.serialize(true);
    }
}
