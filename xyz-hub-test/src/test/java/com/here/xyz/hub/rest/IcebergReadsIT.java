/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_VND_MAPBOX_VECTOR_TILE;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.STREAM_INFO;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.task.IcebergShadowComparison;
import com.here.xyz.hub.task.IcebergShadowComparison.Outcome;
import com.here.xyz.iceberg.IcebergTestTables;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.json.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Reads of versions which were exported to Iceberg are served by the Iceberg connector (needs ICEBERG_READS=ON, see
 * docker-compose-dynamodb.yml) and return the same features as the space's own connector.
 * The Iceberg tables are written by this test with {@link IcebergTestTables} from the features the Hub returns from Postgres,
 * into a JdbcCatalog in the stack's Postgres (schema iceberg_catalog) and the localstack bucket test-bucket.
 */
@TestMethodOrder(OrderAnnotation.class)
public class IcebergReadsIT extends TestSpaceWithFeature {

  private static final String BASE_SPACE_ID = "x-iceberg-reads" + TEST_SUFFIX;
  private static final String COMPOSITE_SPACE_ID = BASE_SPACE_ID + "-ext";
  private static final String FALLBACK_SPACE_ID = BASE_SPACE_ID + "-fallback";
  private static final String ICEBERG_CONNECTOR_ID = "iceberg";
  private static final String CATALOG_SCHEMA = "iceberg_catalog";
  private static final String NAMESPACE = Stream.of("iceberg_it", TEST_SUFFIX.replaceAll("[^A-Za-z0-9]", "").toLowerCase(),
      RandomStringUtils.randomAlphabetic(5).toLowerCase()).filter(part -> !part.isEmpty()).collect(Collectors.joining("_"));
  private static final String TABLE = "base";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(Namespace.of(NAMESPACE), TABLE);
  private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
  private static final String JDBC_USER = "postgres";
  private static final String JDBC_PASSWORD = "password";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int TILE_LEVEL = 13;
  private static final double TILE_LON = 8.03, TILE_LAT = 50.02;

  private static final String V1 = "v1", V2 = "v2", COMPOSITE = "composite";
  private static final String ITERATE = "iterate", BBOX = "bbox", TILE = "tile", GEOMETRY = "geometry", RADIUS = "radius",
      SEARCH = "search";
  private static final List<String> TARGETS = List.of(V1, V2, COMPOSITE);
  private static final List<String> READS = List.of(ITERATE, BBOX, TILE, GEOMETRY, RADIUS, SEARCH);

  private static Catalog catalog;
  private static long v1;
  private static long v2;
  private static long compositeVersion;
  private static long fallbackVersion;
  private static String compositeTag;
  private static final Map<String, List<JsonNode>> baselines = new HashMap<>();
  private static final Map<String, byte[]> mvtBaselines = new HashMap<>();
  private static List<JsonNode> fallbackBaseline;
  private static final Map<String, UUID> referenceIds = new LinkedHashMap<>();
  private static final Set<UUID> deletedReferenceIds = new HashSet<>();

  @BeforeAll
  public static void setup() throws Exception {
    removeSpaces();
    try (Connection connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA IF NOT EXISTS " + CATALOG_SCHEMA);
    }
    catalog = CatalogUtil.buildIcebergCatalog("iceberg", catalogProperties(), null);

    writeSpaces();
    writeIcebergTables();

    //The Postgres baselines, before any ICEBERG reference exists
    for (String target : TARGETS)
      for (String read : READS) {
        ReadResult result = read(spaceOf(target), versionOf(target), read, 1000);
        assertNotRouted(result, target + " " + read);
        assertFalse(result.features().isEmpty(), "The baseline of " + target + " " + read + " is empty");
        baselines.put(target + " " + read, result.features());
      }
    for (String target : TARGETS) {
      Response mvt = mvtTile(spaceOf(target), versionOf(target));
      assertEquals(200, mvt.getStatusCode());
      assertTrue(mvt.getContentType().startsWith(APPLICATION_VND_MAPBOX_VECTOR_TILE), mvt.getContentType());
      assertFalse(streamInfo(mvt).contains("SID=" + ICEBERG_CONNECTOR_ID + ";"));
      mvtBaselines.put(target, mvt.asByteArray());
    }
    fallbackBaseline = read(FALLBACK_SPACE_ID, fallbackVersion, ITERATE, 1000).features();

    referenceIds.put(V1, createReference(BASE_SPACE_ID, v1, location(TABLE, "v1")));
    referenceIds.put(V2, createReference(BASE_SPACE_ID, v2, location(TABLE, "v2")));
    referenceIds.put(COMPOSITE, createReference(COMPOSITE_SPACE_ID, compositeVersion, location(TABLE, compositeTag)));
    referenceIds.put("fallback", createReference(FALLBACK_SPACE_ID, fallbackVersion, location("missing_table", "v1")));
  }

  @AfterAll
  public static void tearDown() {
    for (UUID referenceId : referenceIds.values())
      if (!deletedReferenceIds.contains(referenceId))
        given()
            .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
            .when()
            .delete("/references/" + referenceId);
    removeSpaces();
    if (catalog != null) {
      try {
        catalog.dropTable(TABLE_ID, true);
        if (catalog instanceof SupportsNamespaces namespaces)
          namespaces.dropNamespace(Namespace.of(NAMESPACE));
      }
      finally {
        try {
          if (catalog instanceof AutoCloseable closeable)
            closeable.close();
        }
        catch (Exception ignored) {}
      }
    }
  }

  static Stream<Arguments> routedReads() {
    return TARGETS.stream().flatMap(target -> READS.stream().map(read -> Arguments.of(target, read)));
  }

  @Order(1)
  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("routedReads")
  public void routedReadEqualsPostgres(String target, String read) throws Exception {
    ReadResult result = read(spaceOf(target), versionOf(target), read, 1000);
    assertRouted(result, target + " " + read);
    assertMatch(baselines.get(target + " " + read), result.features(), target + " " + read);
  }

  @Order(2)
  @Test
  public void routedIteratePagesHaveIcebergTokens() throws Exception {
    for (String target : TARGETS) {
      ReadResult result = read(spaceOf(target), versionOf(target), ITERATE, 7);
      assertRouted(result, target + " paged iterate");
      assertTrue(result.tokens().size() >= 2, "Expected several pages for " + target + ", got tokens " + result.tokens());
      for (String token : result.tokens())
        assertTrue(token.startsWith("ice:"), "Not an Iceberg token: " + token);
      Set<String> ids = new HashSet<>();
      for (JsonNode feature : result.features())
        assertTrue(ids.add(feature.get("id").asText()), "Duplicate feature " + feature.get("id") + " in the pages of " + target);
      assertMatch(baselines.get(target + " " + ITERATE), result.features(), target + " paged iterate");
    }
  }

  @Order(3)
  @Test
  public void mvtTileIsNotRouted() {
    //MVT tiles are encoded by the space's own connector, see IcebergReadEligibility
    for (String target : TARGETS) {
      Response mvt = mvtTile(spaceOf(target), versionOf(target));
      assertEquals(200, mvt.getStatusCode());
      assertTrue(mvt.getContentType().startsWith(APPLICATION_VND_MAPBOX_VECTOR_TILE), mvt.getContentType());
      assertFalse(streamInfo(mvt).contains("SID=" + ICEBERG_CONNECTOR_ID + ";"), target + " was routed: " + streamInfo(mvt));
      assertArrayEquals(mvtBaselines.get(target), mvt.asByteArray(), target + ": the MVT differs from the one before the reference");
    }
  }

  @Order(4)
  @Test
  public void missingTableFallsBackToPostgres() throws Exception {
    ReadResult result = read(FALLBACK_SPACE_ID, fallbackVersion, ITERATE, 1000);
    for (String streamInfo : result.streamInfos()) {
      assertTrue(streamInfo.contains("IFB=1;"), "No fallback: " + streamInfo);
      assertFalse(streamInfo.contains("SID=" + ICEBERG_CONNECTOR_ID + ";"), "Routed: " + streamInfo);
    }
    assertMatch(fallbackBaseline, result.features(), "fallback iterate");
  }

  @Order(5)
  @Test
  public void headOfReadOnlySpaceIsRouted() throws Exception {
    setReadOnly(BASE_SPACE_ID);
    ReadResult result = read(BASE_SPACE_ID, null, ITERATE, 1000);
    assertRouted(result, "read-only HEAD");
    assertMatch(baselines.get(V2 + " " + ITERATE), result.features(), "read-only HEAD");
  }

  @Order(6)
  @Test
  public void deletedReferenceStopsRouting() throws Exception {
    UUID referenceId = referenceIds.get(V1);
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .delete("/references/" + referenceId)
        .then()
        .statusCode(204);
    deletedReferenceIds.add(referenceId);

    ReadResult result = read(BASE_SPACE_ID, v1, ITERATE, 1000);
    assertNotRouted(result, "V1 after deleting its reference");
    assertMatch(baselines.get(V1 + " " + ITERATE), result.features(), "V1 after deleting its reference");
  }

  private static void writeSpaces() throws Exception {
    createSpaceWithId(BASE_SPACE_ID, 100);
    List<JsonObject> features = new ArrayList<>();
    for (int i = 0; i < 40; i++)
      features.add(feature(i, momTypeOf(i), 0));
    v1 = putFeatures(BASE_SPACE_ID, features);

    for (int i = 30; i < 35; i++)
      deleteFeature(BASE_SPACE_ID, "f-" + i);
    List<JsonObject> updates = new ArrayList<>();
    for (int i = 0; i < 10; i++)
      //f-2 gets momType A, f-0 loses it, f-3 moves
      updates.add(feature(i, i == 2 ? "A" : i == 0 ? null : momTypeOf(i), i == 3 ? 0.03 : 0).put("n", i + 100));
    v2 = putFeatures(BASE_SPACE_ID, updates);

    createSpace(new JsonObject()
        .put("id", COMPOSITE_SPACE_ID)
        .put("title", "Iceberg reads composite")
        .put("versionsToKeep", 100)
        .put("extends", new JsonObject().put("spaceId", BASE_SPACE_ID).put("version", v2))
        .encode());
    deleteFeature(COMPOSITE_SPACE_ID, "f-5");
    List<JsonObject> extensionFeatures = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      extensionFeatures.add(feature(i, i % 2 == 0 ? "A" : "B", 0.005).put("id", "ext-" + i));
    extensionFeatures.add(feature(6, "A", 0).put("n", 600));
    compositeVersion = putFeatures(COMPOSITE_SPACE_ID, extensionFeatures);
    compositeTag = "ext_" + TABLE + "_b" + v2 + "_v" + compositeVersion;

    createSpaceWithId(FALLBACK_SPACE_ID, 100);
    List<JsonObject> fallbackFeatures = new ArrayList<>();
    for (int i = 0; i < 10; i++)
      fallbackFeatures.add(feature(i, momTypeOf(i), 0));
    fallbackVersion = putFeatures(FALLBACK_SPACE_ID, fallbackFeatures);
  }

  /**
   * Writes the Iceberg table from the features the Hub returns for each version: tag v1 (append), tag v2 (upserts and a deletion
   * vector) and the composite's branch from v2 with its tag.
   */
  private static void writeIcebergTables() throws Exception {
    List<JsonNode> v1Features = read(BASE_SPACE_ID, v1, ITERATE, 1000).features();
    List<JsonNode> v2Features = read(BASE_SPACE_ID, v2, ITERATE, 1000).features();
    List<JsonNode> compositeFeatures = read(COMPOSITE_SPACE_ID, compositeVersion, ITERATE, 1000).features();

    Table table = IcebergTestTables.createTable(catalog, TABLE_ID);
    IcebergTestTables.append(table, null, jsons(v1Features));
    IcebergTestTables.tag(table, "v1");
    IcebergTestTables.upsertAndDelete(table, null, jsons(v2Features), idsOnlyIn(v1Features, v2Features));
    IcebergTestTables.tag(table, "v2");

    String branch = "ext_" + TABLE + "_b" + v2;
    IcebergTestTables.branchFromTag(table, branch, "v2");
    IcebergTestTables.upsertAndDelete(table, branch, jsons(compositeFeatures), idsOnlyIn(v2Features, compositeFeatures));
    IcebergTestTables.tag(table, compositeTag, branch);
  }

  private static Map<String, String> catalogProperties() {
    //The same properties as the connector "iceberg" in connectors.json, but with the host names as seen from the test JVM
    Map<String, String> properties = new HashMap<>();
    properties.put("catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog");
    properties.put("uri", JDBC_URL + "?currentSchema=" + CATALOG_SCHEMA);
    properties.put("jdbc.user", JDBC_USER);
    properties.put("jdbc.password", JDBC_PASSWORD);
    properties.put("warehouse", "s3://test-bucket/iceberg");
    properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    properties.put("s3.endpoint", "http://localhost:4566");
    properties.put("s3.path-style-access", "true");
    properties.put("s3.access-key-id", "localstack");
    properties.put("s3.secret-access-key", "localstack");
    properties.put("client.region", "eu-west-1");
    return properties;
  }

  private static String momTypeOf(int i) {
    return i % 4 == 0 ? "A" : i % 4 == 1 ? "B" : null;
  }

  /**
   * Feature f-i of a grid of 8 x 5 cells (0.01 degrees) north-east of 8.0 / 50.0: points, lines and polygons in turn.
   */
  private static JsonObject feature(int i, String momType, double shift) {
    double lon = 8.0 + (i % 8) * 0.01 + 0.001 + shift;
    double lat = 50.0 + (i / 8) * 0.01 + 0.001 + shift;
    JsonObject geometry = switch (i % 3) {
      case 0 -> new JsonObject().put("type", "Point").put("coordinates", List.of(lon, lat));
      case 1 -> new JsonObject().put("type", "LineString")
          .put("coordinates", List.of(List.of(lon, lat), List.of(lon + 0.004, lat + 0.003)));
      default -> new JsonObject().put("type", "Polygon").put("coordinates", List.of(List.of(List.of(lon, lat),
          List.of(lon + 0.003, lat), List.of(lon + 0.003, lat + 0.003), List.of(lon, lat + 0.003), List.of(lon, lat))));
    };
    JsonObject feature = new JsonObject()
        .put("type", "Feature")
        .put("id", "f-" + i)
        .put("geometry", geometry)
        .put("properties", new JsonObject().put("name", "feature " + i).put("n", i).put("w", i * 0.5)
            .put("nested", new JsonObject().put("even", i % 2 == 0)));
    if (momType != null)
      feature.put("momType", momType);
    return feature;
  }

  /**
   * @return The version of the write
   */
  private static long putFeatures(String spaceId, List<JsonObject> features) throws Exception {
    String body = given()
        .contentType(APPLICATION_GEO_JSON)
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new JsonObject().put("type", "FeatureCollection").put("features", features).encode())
        .when()
        .put(getSpacesPath() + "/" + spaceId + "/features")
        .then()
        .statusCode(200)
        .extract()
        .asString();
    return MAPPER.readTree(body).path("features").get(0).path("properties").path("@ns:com:here:xyz").path("version").asLong();
  }

  private static UUID createReference(String spaceId, long version, String location) {
    String id = given()
        .contentType(APPLICATION_JSON)
        .accept(APPLICATION_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new JsonObject()
            .put("entityId", spaceId)
            .put("isPatch", false)
            .put("endVersion", version)
            .put("objectType", "ICEBERG_TABLE_VERSION")
            .put("contentType", "application/vnd.apache.iceberg")
            .put("location", location)
            .put("sourceSystem", "IML")
            .put("targetSystem", "ICEBERG")
            .put("keepUntil", System.currentTimeMillis() + 24 * 3600 * 1000L)
            .encode())
        .when()
        .post("/references")
        .then()
        .statusCode(201)
        .extract()
        .path("id");
    return UUID.fromString(id);
  }

  private static String location(String table, String ref) {
    return "iceberg://" + NAMESPACE + "/" + table + "?ref=" + ref;
  }

  private static String spaceOf(String target) {
    return COMPOSITE.equals(target) ? COMPOSITE_SPACE_ID : BASE_SPACE_ID;
  }

  private static long versionOf(String target) {
    return switch (target) {
      case V1 -> v1;
      case V2 -> v2;
      default -> compositeVersion;
    };
  }

  private static RequestSpecification request(Long version) {
    RequestSpecification request = given()
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .queryParam("skipCache", true);
    if (version != null)
      request.queryParam("version", version);
    return request;
  }

  /**
   * Runs the read; an iterate follows the page tokens.
   */
  private static ReadResult read(String spaceId, Long version, String read, int limit) throws Exception {
    String path = getSpacesPath() + "/" + spaceId;
    List<JsonNode> features = new ArrayList<>();
    List<String> streamInfos = new ArrayList<>();
    List<String> tokens = new ArrayList<>();
    String token = null;
    do {
      RequestSpecification request = request(version);
      Response response = switch (read) {
        case ITERATE -> {
          request.queryParam("limit", limit);
          if (token != null)
            request.queryParam("handle", token);
          yield request.get(path + "/iterate");
        }
        case BBOX -> request.queryParam("west", 8.0).queryParam("south", 50.0).queryParam("east", 8.045).queryParam("north", 50.025)
            .queryParam("limit", limit).get(path + "/bbox");
        case TILE -> request.queryParam("margin", 20).queryParam("limit", limit).get(path + "/tile/web/" + webTile());
        case GEOMETRY -> request.contentType(APPLICATION_GEO_JSON).queryParam("limit", limit)
            .body("{\"type\":\"Polygon\",\"coordinates\":[[[8.01,50.005],[8.06,50.005],[8.06,50.03],[8.01,50.03],[8.01,50.005]]]}")
            .post(path + "/spatial");
        case RADIUS -> request.queryParam("lat", TILE_LAT).queryParam("lon", TILE_LON).queryParam("radius", 2000)
            .queryParam("limit", limit).get(path + "/spatial");
        case SEARCH -> request.queryParam("f.momType", "A").queryParam("limit", limit).get(path + "/search");
        default -> throw new IllegalArgumentException(read);
      };
      assertEquals(200, response.getStatusCode(), read + " of " + spaceId + ": " + response.asString());
      streamInfos.add(streamInfo(response));
      JsonNode body = MAPPER.readTree(response.asString());
      body.path("features").forEach(features::add);
      token = ITERATE.equals(read) ? pageToken(body) : null;
      if (token != null)
        tokens.add(token);
    }
    while (token != null);
    return new ReadResult(features, streamInfos, tokens);
  }

  private static String pageToken(JsonNode body) {
    JsonNode token = body.hasNonNull("nextPageToken") ? body.get("nextPageToken") : body.get("handle");
    return token == null || token.isNull() ? null : token.asText();
  }

  private static Response mvtTile(String spaceId, long version) {
    return given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .queryParam("skipCache", true)
        .queryParam("version", version)
        .get(getSpacesPath() + "/" + spaceId + "/tile/web/" + webTile() + ".mvt");
  }

  /**
   * The web mercator tile of TILE_LON / TILE_LAT, which covers only a part of the features.
   */
  private static String webTile() {
    int n = 1 << TILE_LEVEL;
    int x = (int) Math.floor((TILE_LON + 180) / 360 * n);
    double latRad = Math.toRadians(TILE_LAT);
    int y = (int) Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * n);
    return TILE_LEVEL + "_" + x + "_" + y;
  }

  private static String streamInfo(Response response) {
    String streamInfo = response.getHeader(STREAM_INFO);
    return streamInfo == null ? "" : streamInfo;
  }

  private static void assertRouted(ReadResult result, String description) {
    for (String streamInfo : result.streamInfos())
      assertTrue(streamInfo.contains("SID=" + ICEBERG_CONNECTOR_ID + ";"), description + " was not routed: " + streamInfo);
  }

  private static void assertNotRouted(ReadResult result, String description) {
    for (String streamInfo : result.streamInfos())
      assertFalse(streamInfo.contains("SID=" + ICEBERG_CONNECTOR_ID + ";"), description + " was routed: " + streamInfo);
  }

  private static void assertMatch(List<JsonNode> expected, List<JsonNode> actual, String description) throws Exception {
    assertNotNull(expected, "No baseline for " + description);
    IcebergShadowComparison.Result result = IcebergShadowComparison.compare(featureCollection(expected), featureCollection(actual),
        Long.MAX_VALUE);
    assertEquals(Outcome.MATCH, result.outcome(), description + ": " + result);
  }

  private static FeatureCollection featureCollection(List<JsonNode> features) throws Exception {
    ObjectNode fc = MAPPER.createObjectNode().put("type", "FeatureCollection");
    ArrayNode array = fc.putArray("features");
    features.forEach(array::add);
    return XyzSerializable.deserialize(fc.toString(), FeatureCollection.class);
  }

  private static List<String> jsons(List<JsonNode> features) {
    return features.stream().map(JsonNode::toString).toList();
  }

  private static List<String> idsOnlyIn(List<JsonNode> features, List<JsonNode> others) {
    Set<String> otherIds = new HashSet<>();
    others.forEach(feature -> otherIds.add(feature.get("id").asText()));
    return features.stream().map(feature -> feature.get("id").asText()).filter(id -> !otherIds.contains(id)).toList();
  }

  private static void removeSpaces() {
    //Undo setReadOnly before the deletion
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .contentType(APPLICATION_JSON)
        .body(new JsonObject().put("readOnly", false).encode())
        .when()
        .patch(getSpacesPath() + "/" + BASE_SPACE_ID);
    removeSpace(COMPOSITE_SPACE_ID);
    removeSpace(BASE_SPACE_ID);
    removeSpace(FALLBACK_SPACE_ID);
  }

  private record ReadResult(List<JsonNode> features, List<String> streamInfos, List<String> tokens) {}
}
