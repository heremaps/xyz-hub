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

package com.here.xyz.psql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.FeatureCollection.ModificationFailure;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.query.EraseSpace;
import com.here.xyz.psql.query.GetFastStatistics;
import com.here.xyz.psql.query.GetFeaturesByBBox;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.psql.query.GetFeaturesById;
import com.here.xyz.psql.query.IterateFeatures;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.psql.query.WriteFeatures;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.psql.query.helpers.versioning.GetNextVersion;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.Random;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKBWriter;
import org.postgresql.util.PGobject;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;
import static com.here.xyz.events.UpdateStrategy.OnExists;
import static com.here.xyz.events.UpdateStrategy.OnNotExists;

import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;

public class NLConnector extends PSQLXyzConnector {
  private static final Logger logger = LogManager.getLogger();
  private static final String STATUS_PROPERTY_KEY = "status";
  private static final String GLOBAL_VERSION_PROPERTY_KEY = "globalVersion";
  private static final String GLOBAL_VERSION_SEARCH_KEY = "globalVersions";
  private static final String REF_QUAD_PROPERTY_KEY = "refQuad";
  private static final String REF_QUAD_COUNT_SELECTION_KEY = "f.refQuadCount";
  //If seedingMode is active, we do not use the FeatureWriter, but a simple batch upsert and delete
  private boolean seedingMode = false;

  @Override
  protected StatisticsResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    //Only support fast statistics
    return run(new GetFastStatistics(event).withTableLayout(NEW_LAYOUT));
  }

  @Override
  protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    return run(new GetFeaturesById(event).withTableLayout(NEW_LAYOUT));
  }

  @Override
  protected FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    return run(new GetFeaturesByGeometry(event).withTableLayout(NEW_LAYOUT));
  }

  @Override
  protected FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    checkForInvalidHereTileClustering(event);
    return getBBox(event);
  }

  private FeatureCollection getBBox(GetFeaturesByBBoxEvent event) throws ErrorResponseException, SQLException {
    if (event.getClusteringType() != null || event.getTweakType() != null)
      throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
    //TODO: Check Cast
    return (FeatureCollection) run(new GetFeaturesByBBox<>(event).withTableLayout(NEW_LAYOUT));
  }

  @Override
  protected FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    return getBBox(event);
  }

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    return write(new ModifySpace(event).withTableLayout(NEW_LAYOUT));
  }

  @Override
  protected FeatureCollection processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    return (FeatureCollection) run(new IterateFeatures<>(event).withTableLayout(NEW_LAYOUT));
  }

  @Override
  protected FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception {
    return writeFeatures(event);
  }

  @Override
  protected FeatureCollection processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    if (event.getPropertiesQuery() != null && !event.getPropertiesQuery().isEmpty()) {
      PropertiesQueryInput propertiesQueryInput = getPropertiesQueryInput(event.getPropertiesQuery());
      String selectionValue = getSelectionValue(event.getSelection());

      List<String> tables = new ArrayList<>();
      tables.add(XyzEventBasedQueryRunner.readTableFromEvent(event));

      if(event.getParams() != null && event.getParams().get("extends") != null) {
        //TODO: check if we need to support hashing
        tables.add(((Map<String, Object>) event.getParams().get("extends")).get("spaceId").toString());
      }

      logger.info("refquad: {}, globalVersion: {}, selection: {}", propertiesQueryInput.refQuad,
              propertiesQueryInput.globalVersions, selectionValue);

      //Get all status counts
      if(propertiesQueryInput.status != null)
        return getStatus(dbSettings.getSchema(), tables, propertiesQueryInput.getOperations());

      if (selectionValue == null) {
        //Get Features by refQuad or globalVersions
        return getFeaturesByRefOrGlobalVersionsQuad(dbSettings.getSchema(),
                tables, propertiesQueryInput, event.getLimit());
      }else {
        //Get FeatureCount by refQuad and quadLevel
        int refQuadLevel = extractRefQuadLevelValue(selectionValue);
        return getFeatureCountByRefQuadAndLevel(dbSettings.getSchema(),
                tables, propertiesQueryInput.refQuad, refQuadLevel);
      }
    }

    throw new IllegalArgumentException("Search without propertiesQuery is not supported in NLConnector!");
  }

  public boolean isSeedingMode() {
    return seedingMode;
  }

  public void setSeedingMode(boolean seedingMode) {
    this.seedingMode = seedingMode;
  }

  public StorageConnector withSeedingMode(boolean seedingMode) {
    setSeedingMode(seedingMode);
    return this;
  }

  private String getSelectionValue(List<String> selection){
    String errorMessageSelection = "Property based search supports only selection=" + REF_QUAD_COUNT_SELECTION_KEY
            + " or selection=" + REF_QUAD_COUNT_SELECTION_KEY + "@[0-32] search in NLConnector!";
    String selectionValue = null;

    if (selection != null) {
      selectionValue = selection.get(0).toString();
      if (selectionValue.equals(REF_QUAD_COUNT_SELECTION_KEY)) {
        return selectionValue;
      }
      if (selectionValue.startsWith(REF_QUAD_COUNT_SELECTION_KEY + "@")) {
        Integer level = extractRefQuadLevelValue(selectionValue);
        if(level == null || (level >=0 && level <= 32)){
          return selectionValue;
        }
      }
      throw new IllegalArgumentException(errorMessageSelection);
    }
    return selectionValue;
  }

  public static int extractRefQuadLevelValue(String selection) {
    if (selection != null && selection.startsWith(REF_QUAD_COUNT_SELECTION_KEY + "@")) {
      try {
        return Integer.parseInt(selection.substring((REF_QUAD_COUNT_SELECTION_KEY + "@").length()));
      } catch (NumberFormatException ignored) {}
    }
    return 0;
  }

  private PropertiesQueryInput getPropertiesQueryInput(PropertiesQuery propertiesQuery) {
    String errorMessageProperties = "Property based search supports only p." + REF_QUAD_PROPERTY_KEY
            + "^=string || globalversions=int|List<int> || status=string searches in NLConnector!";
    String status = null;
    String refQuad = null;
    List<Integer> globalVersions = new ArrayList<>();

    for (PropertyQueryList propertyQueries : propertiesQuery) {
      for (PropertyQuery pq : propertyQueries) {
        String key = pq.getKey();
        QueryOperation operation = pq.getOperation();
        List<Object> values = pq.getValues();

        if (key.equalsIgnoreCase("properties." + REF_QUAD_PROPERTY_KEY)
                && operation.equals(PropertyQuery.QueryOperation.BEGINS_WITH)) {
          if (!(values.get(0) instanceof String))
            throw new IllegalArgumentException("p." + REF_QUAD_PROPERTY_KEY + " must be a single String!");
          refQuad = values.get(0).toString();
        }
        else if (key.equalsIgnoreCase("properties." + GLOBAL_VERSION_SEARCH_KEY)
                && operation.equals(PropertyQuery.QueryOperation.EQUALS)) {
          for(Object v : values){
            if(v instanceof Integer) {
              Integer globalVersion = (Integer) v;
              globalVersions.add(globalVersion);
            }
            else if(v instanceof Long) {
              Long globalVersion = (Long) v;
              globalVersions.add(globalVersion.intValue());
            }
            else if(v instanceof List<?>){
              List<?> globalVersionList = (List<?>) v;
              for(Object vv : globalVersionList){
                if(vv instanceof Integer) {
                  Integer globalVersion = (Integer) vv;
                  globalVersions.add(globalVersion);
                }
                else if(vv instanceof Long) {
                  Long globalVersion = (Long) vv;
                  globalVersions.add(globalVersion.intValue());
                }
                else
                  throw new IllegalArgumentException("Value for 'p." + GLOBAL_VERSION_SEARCH_KEY + "' must be an Integer or a List<Integer>!");
              }
            }
            else
              throw new IllegalArgumentException("Value for 'p." + GLOBAL_VERSION_SEARCH_KEY + "' must be an Integer or a List<Integer>!");
          }
        }
        else if (key.equalsIgnoreCase("properties." + STATUS_PROPERTY_KEY)
                && operation.equals(QueryOperation.EQUALS)) {
          if (!(values.get(0) instanceof String))
            throw new IllegalArgumentException("p." + REF_QUAD_PROPERTY_KEY + " must be a single String!");
          else
            status = values.get(0).toString();
        }
        else {
          throw new IllegalArgumentException(errorMessageProperties);
        }
      }
    }
    return new PropertiesQueryInput(refQuad, globalVersions, status);
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    if(!event.isEraseContent())
      throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
    return run( new EraseSpace(event).withTableLayout(NEW_LAYOUT));
  }

  public static class PropertiesQueryInput {
    private final String refQuad;
    private final String status;
    private final List<Integer> globalVersions;

    public PropertiesQueryInput(String refQuad, List<Integer> globalVersions, String status) {
      this.refQuad = refQuad;
      this.globalVersions = globalVersions;
      this.status = status;
    }

    public String refQuad() {
      return refQuad;
    }

    public List<Integer> globalVersions() {
      return globalVersions;
    }

      public Character[] getOperations(){
          if (status == null || status.isEmpty()) return new Character[0];
          String[] ops = status.split(",");
          Character[] result = new Character[ops.length];
          for (int i = 0; i < ops.length; i++) {
              result[i] = ops[i].trim().charAt(0);
          }
          return result;
      }
  }

  @Override
  protected BinaryResponse processBinaryGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Deprecated
  @Override
  protected SuccessResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  //protected StorageStatistics processGetStorageStatisticsEvent() get used from PSQLXyzConnector

  @Override
  protected SuccessResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected ChangesetCollection processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected ChangesetsStatisticsResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  private FeatureCollection writeFeatures(WriteFeaturesEvent event) throws Exception {
    if(!seedingMode /* TBD: && event.getVersionsToKeep() > 1*/){
      //Use FeatureWriter
      return run(new WriteFeatures(event).withTableLayout(NEW_LAYOUT));
    }

    Set<WriteFeaturesEvent.Modification> modifications = event.getModifications();

    FeatureCollection insertFeatures = new FeatureCollection();
    List<ModificationFailure> fails = new ArrayList<>();

    for (WriteFeaturesEvent.Modification modification : modifications) {
      validateUpdateStrategy(modification.getUpdateStrategy());
      //Batch insert
      insertFeatures.getFeatures().addAll(modification.getFeatureData().getFeatures());
    }

    if(!insertFeatures.getFeatures().isEmpty()) {
      //retrieve Version
      Long version = run(new GetNextVersion<>(event));
      batchInsertFeatures(dbSettings.getSchema(), XyzEventBasedQueryRunner.readTableFromEvent(event),
              insertFeatures, version, event.getAuthor(), fails);
    }

    if(!fails.isEmpty())
      return new FeatureCollection().withFailed(fails);
    return new FeatureCollection();
  }

  private void validateUpdateStrategy(UpdateStrategy updateStrategy) throws ErrorResponseException {
    List<UpdateStrategy.OnExists> supportedOnExistsStrategies = List.of(OnExists.ERROR);
    List<UpdateStrategy.OnNotExists> supportedOnNotExistsStrategies = List.of(OnNotExists.CREATE);

    if(updateStrategy == null || updateStrategy.onExists() == null || updateStrategy.onNotExists() == null)
      logger.error("UpdateStrategy with OnExists and OnNotExists must be provided in NLConnector!");

    if (updateStrategy.onVersionConflict() != null || updateStrategy.onMergeConflict() != null)
      logger.error("onVersionConflict and onMergeConflict are not supported in NLConnector!");
    if(!supportedOnExistsStrategies.contains(updateStrategy.onExists()))
      logger.error("OnExists Strategy '{}' is not supported in NLConnector!", updateStrategy.onExists());
    if(!supportedOnNotExistsStrategies.contains(updateStrategy.onNotExists()))
      logger.error("OnNotExists Strategy '{}' is not supported in NLConnector!", updateStrategy.onNotExists());
  }

  private FeatureCollection getFeaturesByRefOrGlobalVersionsQuad(String schema, List<String> tables,
              PropertiesQueryInput propertiesQueryInput, long limit)  throws SQLException, JsonProcessingException {
    try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {
      String query = createReadByRefQuadOrGlobalVersionsQuery(schema, tables, propertiesQueryInput.refQuad,
              propertiesQueryInput.globalVersions, limit);
      try (PreparedStatement ps = connection.prepareStatement(query)) {
        try (ResultSet rs = ps.executeQuery()) {
          if(rs.next()){
            return XyzSerializable.deserialize(rs.getString("featureCollection"), FeatureCollection.class);
          }
        }
      }catch (SQLException e){
        logger.error(e);
        throw e;
      }
    }
    //should not happen - but for compiler
    return null;
  }

  private FeatureCollection getFeatureCountByRefQuadAndLevel(String schema, List<String> tables, String refQuad, Integer refQuadLevel)
          throws SQLException {
    try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {
      String query = createCountByRefQuadQuery(schema, tables, refQuad, refQuadLevel);

      try (PreparedStatement ps = connection.prepareStatement(query)) {
        try (ResultSet rs = ps.executeQuery()) {
          return getRefQuadCountFc(refQuad, rs);
        }
      }catch (SQLException e){
        logger.error(e);
        throw e;
      }
    }
  }

  private static FeatureCollection getRefQuadCountFc(String refQuad, ResultSet rs) throws SQLException {
    List<Feature> features = new ArrayList<>();
    while(rs.next()){
      String childQuad = rs.getString("child_quad");
      PolygonCoordinates polygonCoordinates = new PolygonCoordinates();

      if(childQuad != null){
        BBox bBox = WebMercatorTile.forQuadkey(childQuad).getBBox(false);
        LinearRingCoordinates lrc = new LinearRingCoordinates();
        lrc.add(new Position(bBox.minLon(), bBox.minLat()));
        lrc.add(new Position(bBox.maxLon(), bBox.minLat()));
        lrc.add(new Position(bBox.maxLon(), bBox.maxLat()));
        lrc.add(new Position(bBox.minLon(), bBox.maxLat()));
        lrc.add(new Position(bBox.minLon(), bBox.minLat()));
        polygonCoordinates.add(lrc);
      }

      features.add(
              new Feature()
                      .withId(childQuad)
                      .withGeometry(
                              refQuad == null ? null : new Polygon().withCoordinates(polygonCoordinates)
                      )
                      .withProperties(new Properties().with("count", rs.getLong("cnt")))
      );
    }
    return new FeatureCollection().withFeatures(features);
  }

  private FeatureCollection getStatus(String schema, List<String> tables, Character[] operations)
          throws SQLException {
    try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {

      String query = createStausQuery(schema, tables, operations);

      try (PreparedStatement ps = connection.prepareStatement(query)) {
        try (ResultSet rs = ps.executeQuery()) {
          Properties properties = new Properties()
                  .with("I",0)
                  .with("U",0)
                  .with("D",0)
                  .with("H",0)
                  .with("J",0);

          while(rs.next()){
            properties.put(rs.getString("operation"),rs.getLong("cnt"));
          }

          return new FeatureCollection().withFeatures(List.of(
                    new Feature()
                            .withId(UUID.randomUUID().toString())
                            .withProperties(properties)));
          }
      }catch (SQLException e){
        logger.error(e);
        throw e;
      }
    }
  }

  private String createCountByRefQuadQuery(String schema, List<String> tables, String refQuad, int quadKeyLevel){
    String extensionTable = tables.get(0);
    String baseTable = tables.size() == 2 ? tables.get(1) : null;

    if(baseTable == null) {
      return """
               WITH params AS (
                   SELECT '$refQuad$'::text AS parent, $quadKeyLevel$ AS relative_level
               )
               SELECT LEFT(searchable->>'refQuad', LENGTH(parent) + relative_level) AS child_quad,
                      COUNT(*) AS cnt
               FROM "$schema$"."$table$", params
               	  WHERE searchable->>'refQuad' LIKE parent || '%'
               GROUP BY child_quad;
              """
              .replace("$refQuad$", refQuad)
              .replace("$quadKeyLevel$", Integer.toString(quadKeyLevel))
              .replace("$schema$", schema)
              .replace("$table$", extensionTable + "_head");
    }
    return """
          WITH params AS (
              SELECT '$refQuad$'::text AS parent, $quadKeyLevel$ AS relative_level
          ),
          combined AS (
            SELECT *
            FROM (
                SELECT
                    id,
                    searchable->>'refQuad' AS refquad,
                    operation,
                    next_version
                FROM "$schema$"."$extensionTable$"
                WHERE operation NOT IN ('D', 'H', 'J')
            )
            UNION ALL
            (
                SELECT *
                FROM (
                    SELECT
                        id,
                        searchable->>'refQuad' AS refquad,
                        operation,
                        next_version
                    FROM "$schema$"."$baseTable$"
                ) base
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM "$schema$"."$extensionTable$"
                    WHERE id = base.id
                      AND next_version = 9223372036854775807::BIGINT
                      AND operation != 'D'
                )
              )
            ),
            filtered AS (
                SELECT c.*
                  FROM combined c
                 WHERE c.refquad LIKE (SELECT parent FROM params) || '%'
            )
            SELECT LEFT(f.refquad, LENGTH(p.parent) + p.relative_level) AS child_quad,
                   COUNT(f.refquad) AS cnt
              FROM filtered f, params p
             GROUP BY child_quad;
          """.replace("$refQuad$", refQuad)
            .replace("$quadKeyLevel$", Integer.toString(quadKeyLevel))
            .replace("$schema$", schema)
            .replace("$baseTable$", baseTable + "_head")
            .replace("$extensionTable$", extensionTable + "_head");
  }

  private String createStausQuery(String schema, List<String> tables, Character[] operations){
    String extensionTable = tables.get(0);
    String baseTable = tables.size() == 2 ? tables.get(1) : null;

    String opsString = "";
    if (operations != null && operations.length > 0) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < operations.length; i++) {
        sb.append("'").append(operations[i].toString().toUpperCase()).append("'");
        if (i < operations.length - 1) sb.append(",");
      }
      opsString = sb.toString();
    }

    if(baseTable == null)
      return """
               SELECT operation, COUNT(*) AS cnt
                FROM "$schema$"."$table$"
               WHERE operation IN($operations$)
               GROUP BY operation;
              """
             .replace("$operations$", opsString)
             .replace("$schema$", schema)
             .replace("$table$", extensionTable + "_head");
    return """
            SELECT operation, COUNT(*) AS cnt
            FROM (
              SELECT operation FROM "$schema$"."$baseTable$"
                WHERE operation IN($operations$)
              UNION ALL
              SELECT operation FROM "$schema$"."$extensionTable$"
                WHERE operation IN($operations$)
            ) AS combined
            GROUP BY operation
            """
            .replace("$operations$", opsString)
            .replace("$schema$", schema)
            .replace("$baseTable$", baseTable + "_head")
            .replace("$extensionTable$", extensionTable + "_head");
  }

  private String createReadByRefQuadOrGlobalVersionsQuery(
          String schema,
          List<String> tables,
          String refQuad,
          List<Integer> globalVersions,
          long limit
  ) {
    String extensionTable = tables.get(0);
    String baseTable = tables.size() == 2 ? tables.get(1) : null;

    // Build the WHERE filter fragment (shared)
    StringBuilder filter = new StringBuilder(
            "WHERE  operation NOT IN (\n" +
            "  SELECT unnest(ARRAY['D','H','J']::CHAR[])\n" +
            ")\n" +
            "AND next_version = 9223372036854775807::BIGINT\n");
    if (refQuad != null) {
      filter.append(" AND searchable->'refQuad' >= to_jsonb('")
              .append(refQuad)
              .append("'::text) AND searchable->'refQuad' < to_jsonb('")
              .append(refQuad)
              .append("4'::text) ");
    }
    if (globalVersions != null && !globalVersions.isEmpty()) {
      String joinedVersions = globalVersions.stream()
              .map(v -> "to_jsonb(" + v + ")")
              .collect(java.util.stream.Collectors.joining(","));
      filter.append(" AND (searchable->'globalVersion') IN (")
              .append(joinedVersions)
              .append(") ");
    }

    // Inner selects (no LIMIT yet, apply after UNION to keep global limit semantics)
    String inner1 = "SELECT * FROM \"" + schema + "\".\"" + extensionTable + "_head\" " + filter;

    String finalQuery;
    if(baseTable == null) {
      finalQuery = inner1;
    }else {
      String inner2 = "SELECT * FROM \"" + schema + "\".\"" + baseTable + "_head\" " + filter;
      String whereNotExistsCondition = """
            WHERE NOT EXISTS (
                  SELECT 1
                  	FROM "$schema$"."$table$"
                  WHERE id = a.id
                    AND next_version = 9223372036854775807::BIGINT
                    AND operation != 'D'
                )
            """
              .replace("$schema$", schema)
              .replace("$table$", extensionTable);
      // UNION ALL combined set
      finalQuery = "(" + inner1 + ") UNION ALL (SELECT * FROM(" + inner2 + ") a " + whereNotExistsCondition + ")";
    }

    // Apply global limit if > 0
    if (limit > 0) {
      finalQuery = finalQuery + " LIMIT " + limit;
    }

    // FeatureCollection aggregation across unioned rows
    String selection =
        "'{ \"type\": \"FeatureCollection\", \"features\": [' ||\n" +
        " COALESCE(\n" +
        "   string_agg(\n" +
        "     regexp_replace(\n" +
        "       regexp_replace(\n" +
        "         jsondata,\n" +
        "         '(\"(@ns:com:here:xyz)\"\\s*:\\s*\\{)',\n" +
        "         '\\1\"version\":' || version || ',\"author\":\"' || coalesce(author, 'ANONYMOUS') || '\",',\n" +
        "         'g'\n" +
        "       ),\n" +
        "       '^{',\n" +
        "       '{' || '\"geometry\":' || coalesce(ST_AsGeoJSON(geo, 8), 'null') || ',',\n" +
        "       'g'\n" +
        "     ),\n" +
        "     ','\n" +
        "   ),\n" +
        "   ''\n" +
        " )\n" +
        " || '] }' AS featureCollection\n";

    return "SELECT " + selection + " FROM (" + finalQuery + ") t";
  }

  private void batchInsertFeatures(
          String schema,
          String table,
          FeatureCollection featureCollection,
          long version,
          String author,
          List<ModificationFailure> fails
  ) throws SQLException, JsonProcessingException {

    String insertSql = String.format(
        "INSERT INTO %s.%s\n" +
        "  (id, geo, operation, author, version, jsondata, searchable)\n" +
        "VALUES (?, ?, ?, ?, ?, ?, ?)\n", schema, table);

    try (Connection connection = dataSourceProvider.getWriter().getConnection()) {
      connection.setAutoCommit(false);

      try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
        for (Feature feature : featureCollection.getFeatures()) {
          Geometry geo = feature.getGeometry();

          if (geo != null) {
            for (Coordinate coord : geo.getJTSGeometry().getCoordinates()) {
              if (Double.isNaN(coord.z))
                coord.z = 0; // avoid NaN
            }
          }

          ensureFeatureId(feature);
          ps.setString(1, feature.getId());
          ps.setBytes(2, geo == null ? null : new WKBWriter(3).write(geo.getJTSGeometry()));
          ps.setString(3, "I"); // operation is always insert
          ps.setString(4, author == null ? "ANONYMOUS" : author);
          ps.setLong(5, version);
          ps.setString(6, enrichFeaturePayload(feature));
          ps.setObject(7, getSearchableObject(feature));
          ps.addBatch();
        }

        int[] inserted = ps.executeBatch();
        connection.commit();
        logger.info("Successfully inserted {} features.", inserted.length);

      } catch (SQLException e) {
        connection.rollback();
        logger.error("Insert failed", e);
        fails.add(new ModificationFailure().withMessage("Insert failed: " + e.getMessage()));
      }
    }
  }

  private void ensureFeatureId(Feature feature) {
    if(feature.getId() == null){
      feature.setId(Random.randomAlphaNumeric(16));
    }
  }

  private PGobject getSearchableObject(Feature feature) throws SQLException {
    PGobject jsonObject = new PGobject();
    jsonObject.setType("jsonb");

    JsonObject searchable = new JsonObject();
    if(feature.getProperties().get(REF_QUAD_PROPERTY_KEY) != null)
      searchable.put(REF_QUAD_PROPERTY_KEY, feature.getProperties().get(REF_QUAD_PROPERTY_KEY));
    if(feature.getProperties().get(GLOBAL_VERSION_PROPERTY_KEY) != null)
      searchable.put(GLOBAL_VERSION_PROPERTY_KEY, feature.getProperties().get(GLOBAL_VERSION_PROPERTY_KEY));
    jsonObject.setValue(searchable.toString());
    return jsonObject;
  }

  private String enrichFeaturePayload(Feature feature) {
    //LFE is missing - so we do not have the createdAt from db | also patch possibility is missing
    //TODO: check if we want to use the same timestamp for all features in one request
    long currentTime = System.currentTimeMillis();
    //Remove Geometry
    feature.setGeometry(null);

    if(feature.getProperties() == null)
      feature.setProperties(new Properties());

    if(feature.getProperties().getXyzNamespace() != null){
      feature.getProperties().getXyzNamespace().setCreatedAt(0);
      //FIXME: remove a version if set
      feature.getProperties().getXyzNamespace().setVersion(-1);
    }else{
      feature.getProperties().setXyzNamespace(new XyzNamespace());
    }
    feature.getProperties().getXyzNamespace().setUpdatedAt(currentTime);
    return feature.serialize().replace("\"geometry\":null,", "");
  }

  /** Currently unsued functions */
  private String getRefQuad(Feature feature) {
    return feature.getProperties().get(REF_QUAD_PROPERTY_KEY);
  }

  private Integer getGlobalVersion(Feature feature) {
    Integer globalVersion = feature.getProperties().get(GLOBAL_VERSION_PROPERTY_KEY);
    return globalVersion == null ? -1 : globalVersion;
  }

  private void batchMergeFeatures(
          String schema,
          String table,
          FeatureCollection featureCollection,
          long version,
          String author,
          List<ModificationFailure> fails
  ) throws SQLException, JsonProcessingException {

    String mergeSql = """
        MERGE INTO %s.%s AS t
        USING (
          VALUES (?, ?, ?, ?, ?, ?, ?)
        ) AS s(id, geo, operation, author, version, jsondata, searchable)
        --ON (t.id = s.id AND t.next_version = 9223372036854775807 AND t.global_version = s.global_version)
        ON (t.id = s.id AND t.version = s.version AND t.next_version = 9223372036854775807)
        WHEN MATCHED THEN
          UPDATE SET
            jsondata  = s.jsondata,
            geo       = s.geo,
            version   = s.version,
            author    = s.author,
            operation = 'U',
            searchable = s.searchable
        WHEN NOT MATCHED THEN
          INSERT (id, geo, operation, author, version, jsondata, searchable)
          VALUES (s.id, s.geo, s.operation, s.author, s.version, s.jsondata, s.searchable);
        """.formatted(schema, table);

    try (Connection connection = dataSourceProvider.getWriter().getConnection()) {
      connection.setAutoCommit(false);

      try (PreparedStatement ps = connection.prepareStatement(mergeSql)) {
        for (Feature feature : featureCollection.getFeatures()) {
          Geometry geo = feature.getGeometry();

          if (geo != null) {
            for (Coordinate coord : geo.getJTSGeometry().getCoordinates()) {
              if (Double.isNaN(coord.z))
                coord.z = 0; // avoid NaN
            }
          }

          ensureFeatureId(feature);
          ps.setString(1, feature.getId());
          ps.setBytes(2, geo == null ? null : new WKBWriter(3).write(geo.getJTSGeometry()));
          ps.setString(3, "I"); // operation always insert initially
          ps.setString(4, author == null ? "ANONYMOUS" : author);
          ps.setLong(5, version);
          ps.setString(6, enrichFeaturePayload(feature));
          ps.setObject(7, getSearchableObject(feature));
          ps.addBatch();
        }

        int[] updated = ps.executeBatch();
        connection.commit();
        logger.info("Successfully upserted {} features.", updated);

      } catch (SQLException e) {
        connection.rollback();
        logger.error("Upsert failed", e);
        fails.add(new ModificationFailure().withMessage("Upsert failed: " + e.getMessage()));
      }
    }
  }

  private void batchDeleteFeatures(String schema, String table, List<String> featureIds,
                                   List<ModificationFailure> fails)
          throws SQLException {
    String deletionSql = "DELETE FROM $table$ WHERE id = ANY(?)".replace("$table$","\""+schema+"\".\""+table+"\"");

    try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {
      try (PreparedStatement ps = connection.prepareStatement(deletionSql)) {
        connection.setAutoCommit(false);

        Array idArray = connection.createArrayOf("text", featureIds.toArray());
        ps.setArray(1, idArray);

        int deleted = ps.executeUpdate();
        if(deleted != featureIds.size()){
          logger.warn("Requested to delete {} features, but only {} are available for deletion!", featureIds.size(), deleted);
          fails.add(new ModificationFailure().withMessage("Deletion failed: not all requested IDs exist."));
          connection.rollback();
        }else {
          logger.info("Successfully deleted {} features.", deleted);
          connection.commit();
        }
      }catch (SQLException e){
        logger.error(e);
      }
    }
  }
}
