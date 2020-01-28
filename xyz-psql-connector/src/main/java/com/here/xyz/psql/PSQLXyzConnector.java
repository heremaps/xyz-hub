/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.CountFeaturesEvent;
import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.TagList;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.WKTHelper;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.FeatureCollection.ModificationFailure;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.CountResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.PropertiesStatistics;
import com.here.xyz.responses.StatisticsResponse.PropertiesStatistics.Searchable;
import com.here.xyz.responses.StatisticsResponse.PropertyStatistics;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzResponse;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.vividsolutions.jts.io.WKBWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

/**
 * The default implementation of an XYZ Hub Lambda connector. This connector is fully featured and used by the MapHub platform to access
 * those spaces that are stored in a a managed DB cluster.
 */
@SuppressWarnings("SqlResolve")
public class PSQLXyzConnector extends PSQLRequestStreamHandler {

  protected static final int STATEMENT_TIMEOUT_SECONDS = 24;

  private static final Logger logger = LogManager.getLogger();

  private static final long EQUATOR_LENGTH = 40_075_016;
  private static final long TILE_SIZE = 256;
  private static final int MAX_PRECISE_STATS_COUNT = 10_000;
  private static final List<String> GEOMETRY_TYPES = Arrays
      .asList("Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon");
  private static Pattern pattern = Pattern.compile("^BOX\\(([-\\d\\.]*)\\s([-\\d\\.]*),([-\\d\\.]*)\\s([-\\d\\.]*)\\)$");
  private static Map<String, Boolean> initialized = new HashMap<>();
  protected Map<String, String> replacements = new HashMap<>();
  private boolean retryAttempted;

  private static SQLQuery getGeoTypesQuery() {
    int system_rows = 1000;

    SQLQuery q = new SQLQuery("SELECT COALESCE(jsonb_agg(t.type), '[]'::jsonb) as geometryTypes");
    q.append("FROM ( SELECT type FROM (values");
    boolean first = true;
    for (String type : GEOMETRY_TYPES) {
      if (!first) {
        q.append(",");
      } else {
        first = false;
      }
      q.append("('" + type + "', EXISTS ( SELECT 1 FROM ${schema}.${table} TABLESAMPLE SYSTEM_ROWS(" + system_rows
          + ") WHERE ST_GeometryType(geo)='ST_" + type + "'))");
    }
    q.append(") a(type,exists) WHERE exists=true) t");

    return q;
  }

  @Override
  public PSQLConfig initializeConfig(Event event, Context context) throws Exception {
    return new PSQLConfig(event, context);
  }

  @Override
  protected void initialize(Event event) throws Exception {
    super.initialize(event);

    final String ecps = PSQLConfig.getECPS(event);
    if (!initialized.containsKey(ecps)) {
      synchronized (this) {
        initialized.put(ecps, true);
      }
    }
    retryAttempted = false;

    replacements.put("idx_serial", "idx_" + config.table(event) + "_serial");
    replacements.put("idx_id", "idx_" + config.table(event) + "_id");
    replacements.put("idx_tags", "idx_" + config.table(event) + "_tags");
    replacements.put("idx_geo", "idx_" + config.table(event) + "_geo");
    replacements.put("idx_createdAt", "idx_" + config.table(event) + "_createdAt");
    replacements.put("idx_updatedAt", "idx_" + config.table(event) + "_updatedAt");
  }

  public static class XyzConnectionCustomizer extends AbstractConnectionCustomizer { // handle initialization per db connection

    private String getSchema(String parentDataSourceIdentityToken) {
      return (String) extensionsForToken(parentDataSourceIdentityToken).get(C3P0EXT_CONFIG_SCHEMA);
    }

    public void onAcquire(Connection c, String pdsIdt) {
      String schema = getSchema(pdsIdt);  // config.schema();
      QueryRunner runner = new QueryRunner();
      try {
        runner.execute(c, "SET enable_seqscan = off;");
        runner.execute(c, "SET statement_timeout = " + (STATEMENT_TIMEOUT_SECONDS * 1000) + " ;");
        runner.execute(c, "SET search_path=" + schema + ",h3,public,topology;");
      } catch (SQLException e) {
        logger.error("Failed to initialize connection " + c + " [" + pdsIdt + "] : {}", e);
      }
      //logger.info("Acquired " + c + " [" + pdsIdt + "]");
    }
  }

  /**
   * A helper method that will test if the table for the space does exist.
   *
   * @return true if the table for the space exists; false otherwise.
   * @throws SQLException if the test fails due to any SQL error.
   */
  private boolean hasTable() throws SQLException {
    QueryRunner run = new QueryRunner(dataSource);
    if (event instanceof HealthCheckEvent) {
      return true;
    }

    PSQLConfig pConfig = (PSQLConfig) config;
    long start = System.currentTimeMillis();
    try (final Connection conn = dataSource.getConnection()) {
      try (final ResultSet rs = conn.getMetaData()
          .getTables(null, pConfig.schema(), pConfig.table(event), new String[]{"TABLE", "VIEW"})) {
        if (rs.next()) {
          long end = System.currentTimeMillis();
          logger.info("{} - Time for table check: " + (end - start) + "ms", streamId);
          return true;
        }
      }
      return false;
    }
  }

  /**
   * A helper method that will ensure that the tables for the space of this event do exist and is up to date, if not it will alter the
   * table.
   *
   * @throws SQLException if the table does not exist and can't be created or alter failed.
   */
  private void ensureSpace() throws SQLException {
    // Note: We can assume that when the table exists, the postgis extensions are installed.
    if (hasTable()) {
      return;
    }

    try (final Connection connection = dataSource.getConnection()) {
      try {
        final String tableName = config.table(event);

        if (connection.getAutoCommit()) {
          connection.setAutoCommit(false);
        }

        try (Statement stmt = connection.createStatement()) {
          String query = "CREATE TABLE ${schema}.${table} (jsondata jsonb, geo geometry(GeometryZ,4326), i SERIAL, geojson jsonb)";
          query = replaceVars(query);
          stmt.addBatch(query);

          query = "CREATE UNIQUE INDEX ${idx_id} ON ${schema}.${table} ((jsondata->>'id'))";
          query = replaceVars(query, replacements);
          stmt.addBatch(query);

          query = "CREATE INDEX ${idx_tags} ON ${schema}.${table} USING gin ((jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops)";
          query = replaceVars(query, replacements);
          stmt.addBatch(query);

          query = "CREATE INDEX ${idx_geo} ON ${schema}.${table} USING gist ((geo))";
          query = replaceVars(query, replacements);
          stmt.addBatch(query);

          query = "CREATE INDEX ${idx_serial} ON ${schema}.${table}  USING btree ((i))";
          query = replaceVars(query, replacements);
          stmt.addBatch(query);

          query = "CREATE INDEX ${idx_updatedAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt'))";
          query = replaceVars(query, replacements);
          stmt.addBatch(query);

          query = "CREATE INDEX ${idx_createdAt} ON ${schema}.${table} USING btree ((jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'))";
          query = replaceVars(query, replacements);
          stmt.addBatch(query);

          stmt.executeBatch();
          connection.commit();
          logger.info("{} - Successfully created table for space '{}'", streamId, event.getSpace());
        }
      } catch (Exception e) {
        final String tableName = config.table(event);
        logger.error("{} - Failed to create table '{}': {}", streamId, tableName, e);
        connection.rollback();
        // check if the table was created in the meantime, by another instance.
        if (hasTable()) {
          return;
        }
        throw new SQLException("Missing table " + sqlQuote(tableName) + " and creation failed: " + e.getMessage(), e);
      }
    }
  }

  protected byte[] resultSetStreamHandler(ResultSet rs) throws SQLException {
    byte[] resultStream = null;
    while (rs.next()) {
      resultStream = rs.getBytes(1);
    }
    return resultStream;
  }

  /**
   * The default handler for the most results.
   *
   * @param rs the result set.
   * @return the generated feature collection from the result set.
   * @throws SQLException when any unexpected error happened.
   */
  protected FeatureCollection resultSetHandler(ResultSet rs) throws SQLException {
    final boolean isIterate = (event instanceof IterateFeaturesEvent);
    long nextHandle = 0;
    StringBuilder sb = new StringBuilder();
    String prefix = "[";
    sb.append(prefix);
    int numFeatures = 0;
    while (rs.next()) {
      sb.append(rs.getString(1));
      String geom = rs.getString(2);
      sb.setLength(sb.length() - 1);
      sb.append(",\"geometry\":");
      sb.append(geom == null ? "null" : geom);
      sb.append("}");
      sb.append(",");

      if (isIterate) {
        numFeatures++;
        nextHandle = rs.getLong(3);
      }
    }
    if (sb.length() > prefix.length()) {
      sb.setLength(sb.length() - 1);
    }
    sb.append("]");

    final FeatureCollection featureCollection = new FeatureCollection();
    featureCollection._setFeatures(sb.toString());
    if (isIterate) {
      if (numFeatures > 0 && numFeatures == ((IterateFeaturesEvent) event).getLimit()) {
        featureCollection.setHandle("" + nextHandle);
      }
    }

    return featureCollection;
  }

  /**
   * handler for delete by tags results.
   *
   * @param rs the result set.
   * @return the generated feature collection from the result set.
   * @throws SQLException when any unexpected error happened.
   */
  private FeatureCollection oldStatesResultSetHandler(ResultSet rs) throws SQLException {
    StringBuilder sb = new StringBuilder();
    String prefix = "[";
    sb.append(prefix);
    while (rs.next()) {
      sb.append("{\"type\":\"Feature\",\"id\":");
      sb.append(rs.getString("id"));
      String geom = rs.getString("geometry");
      if (geom != null) {
        sb.append(",\"geometry\":");
        sb.append(geom);
        sb.append("}");
      }
      sb.append(",");
    }
    if (sb.length() > prefix.length()) {
      sb.setLength(sb.length() - 1);
    }
    sb.append("]");

    final FeatureCollection featureCollection = new FeatureCollection();
    featureCollection._setFeatures(sb.toString());
    return featureCollection;
  }

  @Override
  protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    final List<String> ids = event.getIds();
    if (ids == null || ids.size() == 0) {
      return new FeatureCollection();
    }

    SQLQuery query = new SQLQuery("SELECT");
    query.append(selectJson(event.getSelection()));
    query.append(", geojson FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
        createSQLArray(ids.toArray(new String[ids.size()]), "text"));
    return executeQueryWithRetry(query);
  }

  private FeatureCollection executeQueryWithRetry(SQLQuery query) throws SQLException {
    return executeQueryWithRetry(query, this::resultSetHandler);
  }

  /**
   * Executes the query and reattempt to execute the query, after
   */
  private <T extends XyzResponse> T executeQueryWithRetry(SQLQuery query, ResultSetHandler<T> handler) throws SQLException {
    try {
      return executeQuery(query, handler);
    } catch (Exception e) {
      try {
        if (canRetryAttempt()) {
          return executeQuery(query, handler);
        }
      } catch (Exception e1) {
        throw e;
      }
      throw e;
    }
  }

  private int executeUpdateWithRetry(SQLQuery query) throws SQLException {
    try {
      return executeUpdate(query);
    } catch (Exception e) {
      try {
        if (canRetryAttempt()) {
          return executeUpdate(query);
        }
      } catch (Exception e1) {
        throw e;
      }
      throw e;
    }
  }

  protected FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    return processGetFeaturesByBBoxEvent(event);
  }

  /**** Begin - HEXBIN related section ******/

  private FeatureCollection processHexbinGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event, BBox bbox, boolean isBigQuery,
      Map<String, Object> clusteringParams) throws Exception {

    int zLevel = (event instanceof GetFeaturesByTileEvent ? (int) ((GetFeaturesByTileEvent) event).getLevel() : H3.bbox2zoom(bbox)),
        maxResForLevel = H3.zoom2resolution(zLevel),
        h3res = (clusteringParams != null && clusteringParams.get(H3.HEXBIN_RESOLUTION) != null
            ? Math.min((Integer) clusteringParams.get(H3.HEXBIN_RESOLUTION), maxResForLevel)
            : maxResForLevel);

    String statisticalProperty = (String) clusteringParams.get(H3.HEXBIN_PROPERTY);
    boolean statisticalPropertyProvided = (statisticalProperty != null && statisticalProperty.length() > 0),
        h3cflip = (clusteringParams.get(H3.HEXBIN_POINTMODE) == Boolean.TRUE);

    final String expBboxSql = String
        .format("st_envelope( st_buffer( ST_MakeEnvelope(%f,%f,%f,%f, 4326)::geography, ( 2.5 * edgeLengthM( %d )) )::geometry )",
            bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat(), h3res);

    /*clippedGeo - passed bbox is extended by "margin" on service level */
    String clippedGeo = (!event.getClip() ? "geo" : String
        .format("ST_Intersection(geo,ST_MakeEnvelope(%f,%f,%f,%f,4326) )", bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat())),
        fid = (!event.getClip() ? "h3" : String.format("h3 || %f || %f", bbox.minLon(), bbox.minLat())),
        filterEmptyGeo = (!event.getClip() ? "" : String.format(" and not st_isempty( %s ) ", clippedGeo));

    final SQLQuery searchQuery = generateSearchQuery(event);

    String aggField = (statisticalPropertyProvided ? "jsonb_set('{}'::jsonb, ? , agg::jsonb)::json" : "agg");

    final SQLQuery query = new SQLQuery(String.format(H3.h3sqlBegin, h3res,
        !h3cflip ? "st_centroid(geo)" : "geo",
        h3cflip ? "st_centroid(geo)" : clippedGeo,
        statisticalPropertyProvided ? ", min, max, sum, avg, median" : "",
        zLevel,
        !h3cflip ? "centroid" : "hexagon",
        aggField,
        fid));

    if (statisticalPropertyProvided) {
      ArrayList<String> jpath = new ArrayList<>();
      jpath.add(statisticalProperty);
      query.addParameter(createSQLArray(jpath.toArray(new String[]{}), "text"));
    }

    query.append(expBboxSql);

    if (!statisticalPropertyProvided) {
      query.append(new SQLQuery(String.format(H3.h3sqlMid, h3res, "(0.0)::numeric", zLevel, H3.pxSize)));
    } else {
      ArrayList<String> jpath = new ArrayList<>();
      jpath.add("properties");
      jpath.addAll(Arrays.asList(statisticalProperty.split("\\.")));

      query.append(new SQLQuery(String.format(H3.h3sqlMid, h3res, "(jsondata#>> ?)::numeric", zLevel, H3.pxSize)));
      query.addParameter(createSQLArray(jpath.toArray(new String[]{}), "text"));
    }

    //query.append(" case st_geometrytype(geo) when 'ST_Point' then geo else st_intersection( geo ," ); query.append( expBboxSql ); query.append(" ) end as geo ");
    query.append(" case st_geometrytype(geo) when 'ST_Point' then geo else st_closestpoint( geo, geo ) end as refpt ");
    query.append(" from ${schema}.${table} v where 1 = 1 and geo && ");
    query.append(expBboxSql);
    query.append(" and st_intersects( geo ,");
    query.append(expBboxSql);
    query.append(" ) ");

    if (searchQuery != null) {
      query.append(" and ");
      query.append(searchQuery);
    }

    //query.append("limit ?", 1000);

    query.append(String.format(H3.h3sqlEnd, filterEmptyGeo));
    query.append("LIMIT ?", event.getLimit());

/*
   { final String queryText = query.text();
     final List<Object> queryParameters = query.parameters();
     logger.info("processHexbinGetFeaturesByBBoxEvent: {} - Parameter: {}", queryText, queryParameters);
   }
*/

    return executeQueryWithRetry(query);
  }

  /**** End - HEXBIN related section ******/

  private FeatureCollection processQuadCount(GetFeaturesByBBoxEvent event, BBox bbox, Map<String, Object> clusteringParams)
      throws Exception {
    /** Quadkey calc */
    final int lev = WebMercatorTile.getZoomFromBBOX(bbox);
    double lon2 = bbox.minLon() + ((bbox.maxLon() - bbox.minLon()) / 2);
    double lat2 = bbox.minLat() + ((bbox.maxLat() - bbox.minLat()) / 2);

    final WebMercatorTile tile = WebMercatorTile.getTileFromLatLonLev(lat2, lon2, lev);
    final SQLQuery query;

    SQLQuery propQuery = null;
    String propQuerySQL = null;
    final PropertiesQuery propertiesQuery = event.getPropertiesQuery();
    final int resolution = clusteringParams.get("resolution") != null ? (int) clusteringParams.get("resolution") : 0;
    final String quadMode = clusteringParams.get("quadmode") != null ? (String) clusteringParams.get("quadmode") : null;

    QuadClustering.checkQuadInput(quadMode, resolution, event, streamId, this);

    if (propertiesQuery != null) {
      propQuery = generatePropertiesQuery(propertiesQuery);

      if (propQuery != null) {
        propQuerySQL = propQuery.text();
        for (Object param : propQuery.parameters()) {
          propQuerySQL = propQuerySQL.replaceFirst("\\?", "'" + param + "'");
        }
      }
    }
    return executeQueryWithRetry(
        QuadClustering.generateQuadClusteringSQL(config.schema(), config.table(event), resolution, quadMode, propQuerySQL, tile));
  }

  private FeatureCollection performGeometrySearch(GetFeaturesByGeometryEvent event)
      throws Exception {

    final int radius = event.getRadius();
    final Geometry geometry = event.getGeometry();

    final SQLQuery query;
    final SQLQuery searchQuery = generateSearchQuery(event);

    final SQLQuery geoQuery = radius != 0 ? new SQLQuery("ST_Intersects(geo, ST_Buffer(ST_GeomFromText('"
        + WKTHelper.geometryToWKB(geometry) + "')::geography, ? )::geometry)", radius) : new SQLQuery("ST_Intersects(geo, ST_GeomFromText('"
        + WKTHelper.geometryToWKB(geometry) + "',4326))");

    if (searchQuery == null) {
      query = new SQLQuery("SELECT");
      query.append(selectJson(event.getSelection()));
      /** No clipping or simplification needed*/
      query.append(",geojson");
      query.append("FROM ${schema}.${table} WHERE");
      query.append(geoQuery);
      query.append("LIMIT ?", event.getLimit());
    } else {
      query = getCombinedQuery(event, geoQuery, searchQuery);
    }

    return executeQueryWithRetry(query);
  }

  @Override
  protected FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event)
      throws Exception {
    return performGeometrySearch(event);
  }

  @Override
  protected FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    final BBox bbox = event.getBbox();
    final boolean isBigQuery = (bbox.widthInDegree(false) >= (360d / 4d) || (bbox.heightInDegree() >= (180d / 4d)));

    String clusteringType = event.getClusteringType();
    Map<String, Object> clusteringParams = event.getClusteringParams();

    if (clusteringType != null && H3.HEXBIN.equalsIgnoreCase(clusteringType)) {
      return processHexbinGetFeaturesByBBoxEvent(event, bbox, isBigQuery, clusteringParams);
    } else if (clusteringType != null && QuadClustering.QUAD.equalsIgnoreCase(clusteringType)) {
      return processQuadCount(event, bbox, clusteringParams);
    }

    final SQLQuery searchQuery = generateSearchQuery(event);
    final SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ST_MakeEnvelope(?, ?, ?, ?, 4326))",
        bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());

    final SQLQuery query;
    if (searchQuery == null) {
      query = new SQLQuery("SELECT");
      query.append(selectJson(event.getSelection()));
      query.append(",");
      query.append(geometrySelectorForEvent(event));
      query.append("FROM ${schema}.${table} WHERE");
      query.append(geoQuery);
      query.append("LIMIT ?", event.getLimit());

    } else if (isBigQuery) {
      if (!Capabilities.canSearchFor(event.getSpace(), event.getPropertiesQuery(), this)) {
        throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
            "Invalid request parameters. Search for the provided properties is not supported for this space.");
      }
      query = getCombinedQuery(event, searchQuery, geoQuery);
    } else {
      query = getCombinedQuery(event, geoQuery, searchQuery);
    }

    return executeQueryWithRetry(query);
  }

  private SQLQuery getCombinedQuery(SearchForFeaturesEvent event, SQLQuery indexedQuery, SQLQuery secondaryQuery) throws SQLException {
    final SQLQuery query = new SQLQuery();
    query.append("WITH features(jsondata, geojson, geo) AS (");
    query.append("SELECT jsondata, geojson, geo FROM ${schema}.${table} WHERE");
    query.append(indexedQuery);
    query.append(")");
    query.append("SELECT");
    query.append(selectJson(event.getSelection()));

    if (event instanceof GetFeaturesByBBoxEvent) {
      query.append(",");
      query.append(geometrySelectorForEvent((GetFeaturesByBBoxEvent) event));
    } else {
      query.append(",geojson");
    }

    query.append("FROM features WHERE");
    query.append(secondaryQuery);
    query.append("LIMIT ?", event.getLimit());
    return query;
  }

  /**
   * Returns the query, which will contains the geometry object.
   */
  private SQLQuery geometrySelectorForEvent(final GetFeaturesByBBoxEvent event) {
    final long simplificationLevel = Optional.ofNullable(event.getSimplificationLevel()).orElse(0L);
    final double pixelSize = (double) EQUATOR_LENGTH / (TILE_SIZE << simplificationLevel);

    if (!event.getClip()) {
      if (simplificationLevel <= 0) {
        return new SQLQuery("geojson");

      }
      return new SQLQuery("ST_AsGeoJson(ST_Transform(ST_MakeValid(ST_SnapToGrid(ST_Force2D(ST_Transform(geo,3857)),?)),4326))", pixelSize);
    }

    final BBox bbox = event.getBbox();
    if (simplificationLevel <= 0) {
      return new SQLQuery("ST_AsGeoJson(ST_Intersection(ST_MakeValid(geo),ST_MakeEnvelope(?,?,?,?,4326)))",
          bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
    }

    return new SQLQuery(
        "ST_AsGeoJson(ST_Intersection(ST_Transform(ST_MakeValid(ST_SnapToGrid(ST_Force2D(ST_Transform(geo,3857)),?)),4326),ST_MakeEnvelope(?,?,?,?,4326)))",
        pixelSize, bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
  }

  @Override
  protected XyzResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    final String schema = config.schema();
    final String table = config.table(event);

    SQLQuery query = new SQLQuery("SELECT * from " + schema + ".xyz_statistic_space('" + schema + "','" + table + "')");

    try {
      return executeQueryWithRetry(query, this::getStatisticsResultSetHandler);
    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  /**
   * The result handler for a CountFeatures event.
   *
   * @param rs the result set.
   * @return the feature collection generated from the result.
   */
  private XyzResponse getStatisticsResultSetHandler(ResultSet rs) {
    try {
      rs.next();

      Value<Long> tablesize = XyzSerializable.deserialize(rs.getString("tablesize"), new TypeReference<Value<Long>>() {
      });
      Value<List<String>> geometryTypes = XyzSerializable
          .deserialize(rs.getString("geometryTypes"), new TypeReference<Value<List<String>>>() {
          });
      Value<List<PropertyStatistics>> tags = XyzSerializable
          .deserialize(rs.getString("tags"), new TypeReference<Value<List<PropertyStatistics>>>() {
          });
      PropertiesStatistics properties = XyzSerializable.deserialize(rs.getString("properties"), PropertiesStatistics.class);
      Value<Long> count = XyzSerializable.deserialize(rs.getString("count"), new TypeReference<Value<Long>>() {
      });
      Map<String, Object> bboxMap = XyzSerializable.deserialize(rs.getString("bbox"), new TypeReference<Map<String, Object>>() {
      });

      final String searchable = rs.getString("searchable");
      properties.setSearchable(Searchable.valueOf(searchable));

      String bboxs = (String) bboxMap.get("value");
      if (bboxs == null) {
        bboxs = "";
      }

      BBox bbox = new BBox();
      Matcher matcher = pattern.matcher(bboxs);
      if (matcher.matches()) {
        bbox = new BBox(
            Math.max(-180, Math.min(180, Double.parseDouble(matcher.group(1)))),
            Math.max(-90, Math.min(90, Double.parseDouble(matcher.group(2)))),
            Math.max(-180, Math.min(180, Double.parseDouble(matcher.group(3)))),
            Math.max(-90, Math.min(90, Double.parseDouble(matcher.group(4))))
        );
      }

      return new StatisticsResponse()
          .withBBox(new Value<BBox>().withValue(bbox).withEstimated(bboxMap.get("estimated") == Boolean.TRUE))
          .withByteSize(tablesize)
          .withCount(count)
          .withGeometryTypes(geometryTypes)
          .withTags(tags)
          .withProperties(properties);
    } catch (Exception e) {
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.EXCEPTION).withErrorMessage(e.getMessage());
    }
  }

  @Override
  public XyzResponse processCountFeaturesEvent(CountFeaturesEvent event) throws Exception {
    final SQLQuery searchQuery = generateSearchQuery(event);
    final String schema = config.schema();
    final String table = config.table(event);
    final String schemaTable = sqlQuote(schema) + "." + sqlQuote(table);
    final SQLQuery query;
    if (searchQuery != null) {
      query = new SQLQuery("SELECT count(*) FROM ${schema}.${table} WHERE");
      query.append(searchQuery);
    } else {
      query = new SQLQuery("SELECT CASE WHEN reltuples < 10000");
      query.append("THEN (SELECT count(*) FROM ${schema}.${table})");
      query.append("ELSE reltuples END AS count");
      query.append("FROM pg_class WHERE oid =?::regclass", schemaTable);
    }
    try {
      return executeQueryWithRetry(query, this::countResultSetHandler);
    } catch (SQLException e) {
      // 3F000	INVALID SCHEMA NAME
      // 42P01	UNDEFINED TABLE
      // see: https://www.postgresql.org/docs/current/static/errcodes-appendix.html
      // Note: We know that we're creating the table (and optionally the schema) lazy, that means when a space is created only a
      // corresponding configuration entry is made and only if data is written or read from that space, the schema/table for that space
      // is created, so if the schema and/or space does not exist, we simply assume it is empty.
      if ("42P01".equals(e.getSQLState()) || "3F000".equals(e.getSQLState())) {
        return new CountResponse().withCount(0L).withEstimated(false);
      }
      throw new SQLException(e);
    }
  }

  /**
   * The result handler for a CountFeatures event.
   *
   * @param rs the result set.
   * @return the feature collection generated from the result.
   * @throws SQLException if any error occurred.
   */
  protected XyzResponse countResultSetHandler(ResultSet rs) throws SQLException {
    rs.next();
    long count = rs.getLong(1);
    return new CountResponse().withCount(count).withEstimated(count > MAX_PRECISE_STATS_COUNT);
  }

  @Override
  protected XyzResponse processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    return findFeatures(event, event.getHandle(), true);
  }

  @Override
  protected XyzResponse processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    return findFeatures(event, null, false);
  }

  /**
   * Implementation that is used for the events {@link SearchForFeaturesEvent} and {@link IterateFeaturesEvent}.
   *
   * @return the feature collection with the results.
   */
  private XyzResponse findFeatures(final SearchForFeaturesEvent event, final String handle, final boolean isIterate)
      throws Exception {

    // For testing purposes.
    if (event.getSpace().equals("illegal_argument")) {
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
          .withErrorMessage("Invalid request parameters.");
    }

    if (!Capabilities.canSearchFor(event.getSpace(), event.getPropertiesQuery(), this)) {
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
          .withErrorMessage("Invalid request parameters. Search for the provided properties is not supported for this space.");
    }

    final SQLQuery query = new SQLQuery("SELECT");
    query.append(selectJson(event.getSelection()));
    query.append(", geojson, i FROM ${schema}.${table}");
    final SQLQuery searchQuery = generateSearchQuery(event);
    final boolean hasSearch = searchQuery != null;
    final boolean hasHandle = handle != null;
    final long start = hasHandle ? Long.parseLong(handle) : 0L;

    if (hasSearch || hasHandle) {
      query.append("WHERE");
    }

    if (hasSearch) {
      query.append(searchQuery);
    }

    if (hasHandle) {
      if (hasSearch) {
        query.append("OFFSET ?", start);
      } else {
        query.append("i > ?", start);
      }
    }

    if (isIterate && !hasSearch) {
      query.append("ORDER BY i");
    }

    query.append("LIMIT ?", event.getLimit());

    FeatureCollection collection = executeQueryWithRetry(query);
    if (isIterate && hasSearch && collection.getHandle() != null) {
      collection.setHandle("" + (start + event.getLimit()));
    }

    return collection;
  }

  @Override
  protected XyzResponse processDeleteFeaturesByTagEvent(DeleteFeaturesByTagEvent event) throws Exception {
    if (config.isReadOnly()) {
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.NOT_IMPLEMENTED)
          .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
    }
    final SQLQuery query;
    final SQLQuery searchQuery = generateSearchQuery(event);

    if (searchQuery != null) {
      query = new SQLQuery("DELETE FROM ${schema}.${table} WHERE");
      query.append(searchQuery);
    } else {
      query = new SQLQuery("TRUNCATE ${schema}.${table}");
    }

    boolean includeOldStates = event.getParams() != null && event.getParams().get(PSQLConfig.INCLUDE_OLD_STATES) == Boolean.TRUE;
    if (searchQuery != null && includeOldStates) {
      query.append(" RETURNING jsondata->'id' as id, geojson as geometry");
      return executeQueryWithRetry(query, this::oldStatesResultSetHandler);
    }

    return new FeatureCollection().withCount((long) executeUpdateWithRetry(query));
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    final Map<String, String> idMap = event.getIdsMap();
    if (idMap == null || idMap.size() == 0) {
      return new FeatureCollection();
    }

    final ArrayList<String> ids = new ArrayList<>(idMap.size());
    ids.addAll(idMap.keySet());
    final SQLQuery query = new SQLQuery("SELECT jsondata, geojson FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
        createSQLArray(ids.toArray(new String[ids.size()]), "text"));
    return executeQueryWithRetry(query);
  }

  @Override
  protected XyzResponse processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    if (config.isReadOnly()) {
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.NOT_IMPLEMENTED)
          .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
    }

    final long now = System.currentTimeMillis();
    final boolean addUUID = event.getEnableUUID() && event.getVersion().compareTo("0.2.0") < 0;
    // Update the features to insert
    final List<Feature> inserts = event.getInsertFeatures();
    if (inserts != null) {
      for (Feature feature : inserts) {
        if (feature.getId() == null) {
          feature.setId(RandomStringUtils.randomAlphanumeric(16));
        }
        Feature.finalizeFeature(feature, event.getSpace(), addUUID);
      }
    }

    final List<Feature> updates = event.getUpdateFeatures();
    if (updates != null) {
      for (final Feature feature : updates) {
        Feature.finalizeFeature(feature, event.getSpace(), addUUID);
      }
    }

    return executeModifyFeatures(event);
  }

  private FeatureCollection executeModifyFeatures(ModifyFeaturesEvent event) throws Exception {
    boolean includeOldStates = event.getParams() != null && event.getParams().get(PSQLConfig.INCLUDE_OLD_STATES) == Boolean.TRUE;
    List<Feature> oldFeatures = null;

    List<Feature> inserts = Optional.ofNullable(event.getInsertFeatures()).orElse(new ArrayList<>());
    List<Feature> updates = Optional.ofNullable(event.getUpdateFeatures()).orElse(new ArrayList<>());
    Map<String, String> deletes = Optional.ofNullable(event.getDeleteFeatures()).orElse(new HashMap<>());
    List<ModificationFailure> fails = Optional.ofNullable(event.getFailed()).orElse(new ArrayList<>());
    List<String> insertIds = inserts.stream().map(Feature::getId).filter(Objects::nonNull).collect(Collectors.toList());
    List<String> updateIds = updates.stream().map(Feature::getId).filter(Objects::nonNull).collect(Collectors.toList());
    List<String> deleteIds = new ArrayList<>(deletes.keySet());

    if (includeOldStates) {
      String[] idsToFetch = Stream.of(insertIds, updateIds, deleteIds).flatMap(List::stream).toArray(String[]::new);

      SQLQuery query = new SQLQuery("SELECT jsondata, geojson FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)",
          createSQLArray(idsToFetch, "text"));
      FeatureCollection oldFeaturesCollection = executeQueryWithRetry(query);
      if (oldFeaturesCollection != null) {
        oldFeatures = oldFeaturesCollection.getFeatures();
      }
    }

    try (final Connection connection = dataSource.getConnection()) {
      final FeatureCollection collection = new FeatureCollection();
      collection.setFeatures(new ArrayList<>());
      try {
        boolean transaction = event.getTransaction() == Boolean.TRUE;
        connection.setAutoCommit(!transaction);
        boolean firstConnectionAttempt = true;

        // DELETE
        if (deletes.size() > 0) {
          final ArrayList<String> idsToDelete = new ArrayList<>();

          String deleteAtomicStmtSQL = "DELETE FROM ${schema}.${table} WHERE jsondata->>'id' = ? AND jsondata->'properties'->'@ns:com:here:xyz'->>'hash' = ?";
          deleteAtomicStmtSQL = replaceVars(deleteAtomicStmtSQL);
          boolean batchDeleteAtomic = false;

          try (final PreparedStatement deleteAtomicStmt = createStatement(connection, deleteAtomicStmtSQL)) {
            for (String id : deletes.keySet()) {
              final String hash = deletes.get(id);
              try {
                if (hash == null) {
                  idsToDelete.add(id);
                } else {
                  deleteAtomicStmt.setString(1, id);
                  deleteAtomicStmt.setString(2, hash);
                  if (transaction) {
                    deleteAtomicStmt.addBatch();
                    batchDeleteAtomic = true;
                  } else {
                    deleteAtomicStmt.execute();
                  }
                }
              } catch (Exception e) {
                if (!transaction) {
                  if (firstConnectionAttempt && !retryAttempted) {
                    deleteAtomicStmt.close();
                    connection.close();
                    canRetryAttempt();
                    return executeModifyFeatures(event);
                  }

                  logger.error("{} - Failed to delete object '{}': {}", streamId, id, e);
                } else {
                  throw e;
                }
              }
              firstConnectionAttempt = false;
            }
            if (batchDeleteAtomic) {
              deleteAtomicStmt.executeBatch();
            }
          } catch (Exception dex) {
            throw dex;
          }

          if (idsToDelete.size() > 0) {
            String deleteStmtSQL = "DELETE FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(?)";
            deleteStmtSQL = replaceVars(deleteStmtSQL);
            try (final PreparedStatement deleteStmt = createStatement(connection, deleteStmtSQL)) {
              try {
                deleteStmt.setArray(1, connection.createArrayOf("text", idsToDelete.toArray(new String[idsToDelete.size()])));
                deleteStmt.execute();
              } catch (Exception e) {
                if (!transaction) {
                  if (firstConnectionAttempt && !retryAttempted) {
                    deleteStmt.close();
                    connection.close();
                    canRetryAttempt();
                    return executeModifyFeatures(event);
                  }

                  logger.error("{} - Failed to delete objects {}: {}", streamId, idsToDelete, e);
                } else {
                  throw e;
                }
              }
            }
          }
        }

        // INSERT
        if (inserts.size() > 0) {
          String insertStmtSQL = "INSERT INTO ${schema}.${table} (jsondata, geo, geojson) VALUES(?::jsonb, ST_Force3D(ST_GeomFromWKB(?,4326)), ?::jsonb)";
          insertStmtSQL = replaceVars(insertStmtSQL);
          boolean batchInsert = false;

          String insertWithoutGeometryStmtSQL = "INSERT INTO ${schema}.${table} (jsondata, geo, geojson) VALUES(?::jsonb, NULL, NULL)";
          insertWithoutGeometryStmtSQL = replaceVars(insertWithoutGeometryStmtSQL);
          boolean batchInsertWithoutGeometry = false;

          try (
              final PreparedStatement insertStmt = createStatement(connection, insertStmtSQL);
              final PreparedStatement insertWithoutGeometryStmt = createStatement(connection, insertWithoutGeometryStmtSQL);
          ) {
            for (int i = 0; i < inserts.size(); i++) {

              try {
                final Feature feature = inserts.get(i);
                final Geometry geometry = feature.getGeometry();
                feature.setGeometry(null); // Do not serialize the geometry in the JSON object

                final String json;
                final String geojson;
                try {
                  json = feature.serialize();
                  geojson = geometry != null ? geometry.serialize() : null;
                } finally {
                  feature.setGeometry(geometry);
                }

                final PGobject jsonbObject = new PGobject();
                jsonbObject.setType("jsonb");
                jsonbObject.setValue(json);

                final PGobject geojsonbObject = new PGobject();
                geojsonbObject.setType("jsonb");
                geojsonbObject.setValue(geojson);

                if (geometry == null) {
                  insertWithoutGeometryStmt.setObject(1, jsonbObject);
                  if (transaction) {
                    insertWithoutGeometryStmt.addBatch();
                    batchInsertWithoutGeometry = true;
                  } else {
                    insertWithoutGeometryStmt.execute();
                  }
                } else {
                  insertStmt.setObject(1, jsonbObject);
                  final WKBWriter wkbWriter = new WKBWriter(3);
                  insertStmt.setBytes(2, wkbWriter.write(geometry.getJTSGeometry()));
                  insertStmt.setObject(3, geojsonbObject);

                  if (transaction) {
                    insertStmt.addBatch();
                    batchInsert = true;
                  } else {
                    insertStmt.execute();
                  }
                }
                collection.getFeatures().add(feature);
              } catch (Exception e) {
                if (!transaction) {
                  if (firstConnectionAttempt && !retryAttempted) {
                    insertStmt.close();
                    insertWithoutGeometryStmt.close();
                    connection.close();
                    canRetryAttempt();
                    return executeModifyFeatures(event);
                  }
                  logger.error("{} - Failed to insert object #{}: {}", streamId, i, e);
                } else {
                  throw e;
                }
              }
              firstConnectionAttempt = false;
            }
            if (batchInsert) {
              insertStmt.executeBatch();
            }
            if (batchInsertWithoutGeometry) {
              insertWithoutGeometryStmt.executeBatch();
            }
          } catch (Exception iex) {
            throw iex;
          }
        }

        // UPDATE
        if (updates.size() > 0) {
          String updateStmtSQL = "UPDATE ${schema}.${table} SET jsondata = ?::jsonb, geo=ST_Force3D(ST_GeomFromWKB(?,4326)), geojson = ?::jsonb WHERE jsondata->>'id' = ?";
          updateStmtSQL = replaceVars(updateStmtSQL);
          boolean batchUpdate = false;

          String updateWithoutGeometryStmtSQL = "UPDATE ${schema}.${table} SET  jsondata = ?::jsonb, geo=NULL, geojson = NULL WHERE jsondata->>'id' = ?";
          updateWithoutGeometryStmtSQL = replaceVars(updateWithoutGeometryStmtSQL);
          boolean batchUpdateWithoutGeometry = false;

          try (
              final PreparedStatement updateStmt = createStatement(connection, updateStmtSQL);
              final PreparedStatement updateWithoutGeometryStmt = createStatement(connection, updateWithoutGeometryStmtSQL);
          ) {
            for (int i = 0; i < updates.size(); i++) {
              try {
                final Feature feature = updates.get(i);
                if (feature.getId() == null) {
                  throw new NullPointerException("id");
                }
                final String id = feature.getId();
                final Geometry geometry = feature.getGeometry();
                feature.setGeometry(null); // Do not serialize the geometry in the JSON object

                final String json;
                final String geojson;
                try {
                  json = feature.serialize();
                  geojson = geometry != null ? geometry.serialize() : null;
                } finally {
                  feature.setGeometry(geometry);
                }
                final PGobject jsonbObject = new PGobject();
                jsonbObject.setType("jsonb");
                jsonbObject.setValue(json);

                final PGobject geojsonbObject = new PGobject();
                geojsonbObject.setType("jsonb");
                geojsonbObject.setValue(geojson);

                if (geometry == null) {
                  updateWithoutGeometryStmt.setObject(1, jsonbObject);
                  updateWithoutGeometryStmt.setString(2, id);
                  if (transaction) {
                    updateWithoutGeometryStmt.addBatch();
                    batchUpdateWithoutGeometry = true;
                  } else {
                    updateWithoutGeometryStmt.execute();
                  }
                } else {
                  updateStmt.setObject(1, jsonbObject);
                  final WKBWriter wkbWriter = new WKBWriter(3);
                  updateStmt.setBytes(2, wkbWriter.write(geometry.getJTSGeometry()));
                  updateStmt.setObject(3, geojsonbObject);
                  updateStmt.setString(4, id);
                  if (transaction) {
                    updateStmt.addBatch();
                    batchUpdate = true;
                  } else {
                    updateStmt.execute();
                  }
                }
                collection.getFeatures().add(feature);
              } catch (Exception e) {
                if (!transaction) {
                  if (firstConnectionAttempt && !retryAttempted) {
                    updateStmt.close();
                    updateWithoutGeometryStmt.close();
                    connection.close();
                    canRetryAttempt();
                    return executeModifyFeatures(event);
                  }
                  logger.error("{} - Failed to update object #{}: {}", streamId, i, e);
                } else {
                  throw e;
                }
              }
              firstConnectionAttempt = false;
            }
            if (batchUpdate) {
              updateStmt.executeBatch();
            }
            if (batchUpdateWithoutGeometry) {
              updateWithoutGeometryStmt.executeBatch();
            }
          }
        }

        if (transaction) {
          connection.commit();
        }
      } catch (Exception e) {
        try {
          if (event.getTransaction()) {
            connection.rollback();
            if (!retryAttempted) {
              connection.close();
              canRetryAttempt();
              return executeModifyFeatures(event);
            }
          }
        } catch (Exception e2) {
          logger.error("{} - Unexpected exception while invoking a rollback: {}", streamId, e2);
        }
        logger.error("{} - Failed to execute modify features: {}", streamId, e);
        if (e instanceof SQLException) {
          throw e;
        } else {
          throw new SQLException("Unexpected exception while trying to execute modify features event", e);
        }
      }

      collection.setFailed(fails);

      if (insertIds.size() > 0) {
        if (collection.getInserted() == null) {
          collection.setInserted(new ArrayList<>());
        }
        collection.getInserted().addAll(insertIds);
      }

      if (updateIds.size() > 0) {
        if (collection.getUpdated() == null) {
          collection.setUpdated(new ArrayList<>());
        }
        collection.getUpdated().addAll(updateIds);
      }

      if (deleteIds.size() > 0) {
        if (collection.getDeleted() == null) {
          collection.setDeleted(new ArrayList<>());
        }
        collection.getDeleted().addAll(deleteIds);
      }

      if (oldFeatures != null) {
        collection.setOldFeatures(oldFeatures);
        ;
      }

      return collection;
    }
  }

  private boolean canRetryAttempt() throws Exception {
    if (retryAttempted) {
      return false;
    }
    if (hasTable()) {
      retryAttempted = true; // the table is there, do not retry
      return false;
    }

    ensureSpace();
    retryAttempted = true;
    logger.info("{} - The table was created. Retry the execution.", streamId);
    return true;
  }

  private PreparedStatement createStatement(Connection connection, String statement) throws SQLException {
    final PreparedStatement preparedStatement = connection.prepareStatement(statement);
    preparedStatement.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
    return preparedStatement;
  }

  private String getOperation(QueryOperation op) {
    if (op == null) {
      throw new NullPointerException("op is required");
    }

    switch (op) {
      case EQUALS:
        return "=";
      case NOT_EQUALS:
        return "<>";
      case LESS_THAN:
        return "<";
      case GREATER_THAN:
        return ">";
      case LESS_THAN_OR_EQUALS:
        return "<=";
      case GREATER_THAN_OR_EQUALS:
        return ">=";
    }

    return "";
  }

  private SQLQuery createKey(String key) {
    String[] results = key.split("\\.");
    return new SQLQuery(
        "jsondata->" + Collections.nCopies(results.length, "?").stream().collect(Collectors.joining("->")), results);
  }

  private String getValue(Object value) {
    if (value instanceof String) {
      return "to_jsonb(?::text)";
    }
    if (value instanceof Number) {
      return "to_jsonb(?::numeric)";
    }
    if (value instanceof Boolean) {
      return "to_jsonb(?::boolean)";
    }
    return "";
  }

  private SQLQuery generatePropertiesQuery(PropertiesQuery properties) {
    if (properties == null || properties.size() == 0) {
      return null;
    }
    // TODO: This is only a hot-fix for the connector. The issue is caused in the service and the code below will be removed after the next XYZ Hub deployment
    if (properties.get(0).size() == 0 || properties.get(0).size() == 1 && properties.get(0).get(0) == null) {
      return null;
    }
    // List with the outer OR combined statements
    List<SQLQuery> disjunctionQueries = new ArrayList<>();
    properties.forEach(conjunctions -> {

      // List with the AND combined statements
      final List<SQLQuery> conjunctionQueries = new ArrayList<>();
      conjunctions.forEach(propertyQuery -> {

        // List with OR combined statements for one property key
        final List<SQLQuery> keyDisjunctionQueries = new ArrayList<>();
        propertyQuery.getValues().forEach(v -> {

          // The ID is indexed as text
          if (propertyQuery.getKey().equals("id")) {
            keyDisjunctionQueries.add(new SQLQuery("jsondata->>'id'" + getOperation(propertyQuery.getOperation()) + "?::text", v));
          }
          // The rest are indexed as jsonb
          else {
            SQLQuery q = createKey(propertyQuery.getKey());
            q.append(new SQLQuery(getOperation(propertyQuery.getOperation()) + getValue(v), v));
            keyDisjunctionQueries.add(q);
          }
        });
        conjunctionQueries.add(SQLQuery.join(keyDisjunctionQueries, "OR", true));
      });
      disjunctionQueries.add(SQLQuery.join(conjunctionQueries, "AND", false));
    });
    return SQLQuery.join(disjunctionQueries, "OR", false);
  }

  private SQLQuery generateTagsQuery(TagsQuery tags) throws Exception {
    if (tags == null || tags.size() == 0) {
      return null;
    }

    SQLQuery query;
    StringBuilder andQuery = new StringBuilder("jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??& ?");
    boolean hasAnd = tags.get(0).size() > 1;

    for (int i = 1; i < tags.size(); i++) {
      if (tags.get(i).size() > 1) {
        hasAnd = true;
      }
      andQuery.append(" OR jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??& ?");
    }

    if (!hasAnd) {
      String[] orList = new String[tags.size()];
      for (int i = 0; i < tags.size(); i++) {
        orList[i] = tags.get(i).get(0);
      }

      query = new SQLQuery(" (jsondata->'properties'->'@ns:com:here:xyz'->'tags' ??| ?)", createSQLArray(orList, "text"));
    } else {
      query = new SQLQuery("(" + andQuery + ")");
      for (TagList tag : tags) {
        query.addParameter(createSQLArray(tag.toArray(new String[tag.size()]), "text"));
      }
    }

    return query;
  }

  @Override
  protected SQLQuery generateSearchQuery(final QueryEvent event) throws Exception {
    final SQLQuery propertiesQuery = generatePropertiesQuery(event.getPropertiesQuery());
    final SQLQuery tagsQuery = generateTagsQuery(event.getTags());

    return SQLQuery.join("AND", propertiesQuery, tagsQuery);
  }

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {

    if ((Operation.UPDATE == event.getOperation() || Operation.CREATE == event.getOperation()) && event.getConnectorParams()
        .get("propertySearch") == Boolean.TRUE) {

      if (event.getSpaceDefinition().getSearchableProperties() != null) {
        int cnt = 0;
        for (String property : event.getSpaceDefinition().getSearchableProperties().keySet()) {
          if (event.getSpaceDefinition().getSearchableProperties().get(property) != null
              && event.getSpaceDefinition().getSearchableProperties().get(property) == Boolean.TRUE) {
            cnt++;
          }
          if (cnt > ON_DEMAND_IDX_LIM) {
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                "On-Demand-Indexing - Maximum permissible: " + ON_DEMAND_IDX_LIM + " searchable properties per space!");
          }

          if (property.contains("'")) {
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                "On-Demand-Indexing [" + property + "] - Character ['] not allowed!");
          }
          if (property.contains("\\")) {
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                "On-Demand-Indexing [" + property + "] - Character [\\] not allowed!");
          }
        }
      }
      processSearchableProperties(event.getSpaceDefinition().getSearchableProperties(), event.getOperation());
    }

    if (Operation.DELETE == event.getOperation()) {
      if (hasTable()) {
        try (final Connection connection = dataSource.getConnection()) {
          try (Statement stmt = connection.createStatement()) {
            String query = "DROP TABLE ${schema}.${table}";
            query = replaceVars(query);
            stmt.executeUpdate(query);

            logger.info("{} - Successfully deleted table for space '{}'", streamId, event.getSpace());
          } catch (Exception e) {
            final String tableName = config.table(event);
            logger.error("{} - Failed to delete table '{}': {}", streamId, tableName, e);
            throw new SQLException("Failed to delete table: " + tableName, e);
          }
        }
      } else {
        logger.info("{} - Table not found '{}'", streamId, event.getSpace());
      }
    }
    return new SuccessResponse().withStatus("OK");
  }

  private void processSearchableProperties(Map<String, Boolean> searchableProperties, Operation operation) throws SQLException {
    String searchablePropertiesJson = "";

    if (searchableProperties == null) {
      /** Received an empty map */
      searchableProperties = new HashMap<String, Boolean>();
    } else {
      for (String property : searchableProperties.keySet()) {
        searchablePropertiesJson += "\"" + property + "\":" + searchableProperties.get(property) + ",";
      }
      /** remove last comma */
      searchablePropertiesJson = searchablePropertiesJson.substring(0, searchablePropertiesJson.length() - 1);
    }

    /** update xyz_idx_status table with searchabelProperties information */
    String updateIdxStatusQuery =
        "INSERT INTO xyz_config.xyz_idxs_status as x_s (spaceid,schem,idx_creation_finished,idx_manual) "
            + "		VALUES ('" + config.table(event) + "', '" + config.schema() + "', false, '{" + searchablePropertiesJson
            + "}'::jsonb) "
            + "ON CONFLICT (spaceid) DO "
            + "		UPDATE SET schem='" + config.schema() + "', "
            + "    			idx_manual = '{" + searchablePropertiesJson + "}'::jsonb, "
            + "				idx_creation_finished = false "
            + "		WHERE x_s.spaceid = '" + config.table(event) + "'";

    try (final Connection connection = dataSource.getConnection()) {
      try (Statement stmt = connection.createStatement()) {
        stmt.execute(updateIdxStatusQuery);
      } catch (Exception e) {
        logger.error("{} - Error '{}': {}", streamId, config.table(event), e);
        throw new SQLException("Error " + config.table(event), e);
      }
    }
  }

  private SQLQuery selectJson(List<String> selection) throws SQLException {
    if (selection == null) {
      return new SQLQuery("jsondata");
    }
    if (selection != null && !selection.contains("type")) {
      selection.add("type");
    }

    return new SQLQuery("prj_build(?,jsondata)", createSQLArray(selection.toArray(new String[0]), "text"));
  }
}
