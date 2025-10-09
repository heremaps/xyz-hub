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
import java.util.Set;
import java.util.stream.Collectors;

import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;
import static com.here.xyz.events.UpdateStrategy.OnExists;
import static com.here.xyz.events.UpdateStrategy.OnNotExists;

import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;

public class NLConnector extends PSQLXyzConnector {
  private static final Logger logger = LogManager.getLogger();
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
      PropertiesQueryInput propertiesQueryInput = getRefQuadAndGlobalVersion(event.getPropertiesQuery());
      String selectionValue = getSelectionValue(event.getSelection());

      logger.info("refquad: {}, globalVersion: {}, selection: {}", propertiesQueryInput.refQuad,
              propertiesQueryInput.globalVersions, selectionValue);

      if (selectionValue == null) {
        return getFeaturesByRefOrGlobalVersionsQuad(dbSettings.getSchema(), XyzEventBasedQueryRunner.readTableFromEvent(event),
                propertiesQueryInput, event.getLimit());
      }else {
        return getFeatureCountByRefQuadOrGlobalVersions(dbSettings.getSchema(),
                XyzEventBasedQueryRunner.readTableFromEvent(event),
                propertiesQueryInput.refQuad, propertiesQueryInput.globalVersions, event.getLimit());
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
            + " search in NLConnector!";
    String selectionValue = null;

    if (selection != null && !selection.contains(REF_QUAD_COUNT_SELECTION_KEY))
      throw new IllegalArgumentException(errorMessageSelection);
    else if (selection != null) {
      selectionValue = selection.get(0).toString();
      if (!selectionValue.equals(REF_QUAD_COUNT_SELECTION_KEY))
        throw new IllegalArgumentException(errorMessageSelection);
    }

    return selectionValue;
  }

  private PropertiesQueryInput getRefQuadAndGlobalVersion(PropertiesQuery propertiesQuery) {
    String errorMessageProperties = "Property based search supports only p." + REF_QUAD_PROPERTY_KEY
            + "^=... (and optional globalVersions=...) search in NLConnector!";
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
            if(v instanceof Integer globalVersion)
              globalVersions.add(globalVersion);
            else if(v instanceof Long globalVersion)
              globalVersions.add(globalVersion.intValue());
            else if(v instanceof List<?> globalVersionList){
              for(Object vv : globalVersionList){
                if(vv instanceof Integer globalVersion)
                  globalVersions.add(globalVersion);
                else if(vv instanceof Long globalVersion)
                  globalVersions.add(globalVersion.intValue());
                else
                  throw new IllegalArgumentException("Value for 'p." + GLOBAL_VERSION_SEARCH_KEY + "' must be an Integer or a List<Integer>!");
              }
            }
            else
              throw new IllegalArgumentException("Value for 'p." + GLOBAL_VERSION_SEARCH_KEY + "' must be an Integer or a List<Integer>!");
          }
        }
        else {
          throw new IllegalArgumentException(errorMessageProperties);
        }
      }
    }
    return new PropertiesQueryInput(refQuad, globalVersions);
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    if(!event.isEraseContent())
      throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
    return run( new EraseSpace(event).withTableLayout(NEW_LAYOUT));
  }

  public record PropertiesQueryInput(String refQuad, List<Integer> globalVersions) {}


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
      return run(new WriteFeatures(
              event.withMinVersion(event.getMinVersion())
                      .withVersionsToKeep(100_000)
                      .withResponseDataExpected(false)
      ).withTableLayout(NEW_LAYOUT));
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
      throw new ErrorResponseException(NOT_IMPLEMENTED, "UpdateStrategy with OnExists and OnNotExists must be provided in NLConnector!");

    if (updateStrategy.onVersionConflict() != null || updateStrategy.onMergeConflict() != null)
      throw new ErrorResponseException(NOT_IMPLEMENTED, "onVersionConflict and onMergeConflict are not supported in NLConnector!");
    if(!supportedOnExistsStrategies.contains(updateStrategy.onExists()))
      throw new ErrorResponseException(NOT_IMPLEMENTED, "OnExists Strategy '"+updateStrategy.onExists()+"' is not supported in NLConnector!");
    if(!supportedOnNotExistsStrategies.contains(updateStrategy.onNotExists()))
      throw new ErrorResponseException(NOT_IMPLEMENTED, "OnNotExists Strategy '"+updateStrategy.onNotExists()+"' is not supported in NLConnector!");
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

  private FeatureCollection getFeaturesByRefOrGlobalVersionsQuad(String schema, String table, PropertiesQueryInput propertiesQueryInput, long limit)
          throws SQLException, JsonProcessingException {
    try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {
      String query = createReadByRefQuadOrGlobalVersionsQuery(schema, table, propertiesQueryInput.refQuad, propertiesQueryInput.globalVersions, limit);

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

  private FeatureCollection getFeatureCountByRefQuadOrGlobalVersions(String schema, String table, String refQuad, List<Integer> globalVersions, long limit)
          throws SQLException {
    try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {

      String query = createReadByRefQuadOrGlobalVersisonsCountQuery(schema, table, refQuad, globalVersions, limit);

      try (PreparedStatement ps = connection.prepareStatement(query)) {

        try (ResultSet rs = ps.executeQuery()) {
          if(rs.next()){
            PolygonCoordinates polygonCoordinates = new PolygonCoordinates();

            if(refQuad != null){
              BBox bBox = WebMercatorTile.forQuadkey(refQuad).getBBox(false);
              LinearRingCoordinates lrc = new LinearRingCoordinates();
              lrc.add(new Position(bBox.minLon(), bBox.minLat()));
              lrc.add(new Position(bBox.maxLon(), bBox.minLat()));
              lrc.add(new Position(bBox.maxLon(), bBox.maxLat()));
              lrc.add(new Position(bBox.minLon(), bBox.maxLat()));
              lrc.add(new Position(bBox.minLon(), bBox.minLat()));
              polygonCoordinates.add(lrc);
            }

            return new FeatureCollection().withFeatures(List.of(
                    new Feature()
                       .withId(refQuad)
                       .withGeometry(
                               refQuad == null ? null : new Polygon().withCoordinates(polygonCoordinates)
                       )
                       .withProperties(
                            new Properties()
                                    .with("count", rs.getLong("cnt")
                            ))
            ));
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

  private String createReadByRefQuadOrGlobalVersisonsCountQuery(String schema, String table, String refQuad, List<Integer> globalVersions, long limit) {
    return createReadByRefQuadOrGlobalVersionsQuery(schema, table, refQuad, globalVersions, limit, true);
  }

  private String createReadByRefQuadOrGlobalVersionsQuery(String schema, String table, String refQuad, List<Integer> globalVersions, long limit) {
    return createReadByRefQuadOrGlobalVersionsQuery(schema, table, refQuad, globalVersions, limit, false);
  }

  private String createReadByRefQuadOrGlobalVersionsQuery(String schema, String table, String refQuad, List<Integer> globalVersions,
                                                          long limit, boolean returnCount) {
    StringBuilder innerSelect = new StringBuilder();
    innerSelect.append("SELECT * FROM \"")
            .append(schema).append("\".\"")
            .append(table + "_head").append("\" ")
            .append("WHERE 1=1 ");
            //.append("WHERE operation NOT IN ('D') AND next_version = "+Long.MAX_VALUE+" ");

    if (refQuad != null) {
      innerSelect.append("AND searchable->'refQuad' >= to_jsonb('")
              .append(refQuad)
              .append("'::text) AND searchable->'refQuad' < to_jsonb('")
              .append(refQuad)
              .append("4'::text)");
    }

    if (globalVersions != null && !globalVersions.isEmpty()) {
      //Maybe use "AND searchable->'globalVersion' <@ to_jsonb(ARRAY[1,2,3])" + GIN Index
      String joinedVersions = globalVersions.stream()
              .map(v -> "to_jsonb(" + v + ")")
              .collect(Collectors.joining(","));
      innerSelect.append(" AND (searchable->'globalVersion') IN (")
              .append(joinedVersions)
              .append(")");
    }

    innerSelect.append(" LIMIT ").append(limit);

    String selection;
    if (returnCount) {
      selection = "count(1) as cnt";
    } else {
      selection = """
            '{ "type": "FeatureCollection", "features": [' ||
             COALESCE(
               string_agg(
                 regexp_replace(
                   regexp_replace(
                     -- Inject version and author into namespace
                     jsondata,
                     '("(@ns:com:here:xyz)"\\s*:\\s*\\{)',
                     '\\1"version":' || version || ',"author":"' || coalesce(author, 'ANONYMOUS') || '",',
                     'g'
                   ),
                   -- Add geometry at root level (after first {)
                   '^{',
                   '{' || '"geometry":' || coalesce(ST_AsGeoJSON(geo, 8), 'null') || ',',
                   'g'
                 ),
                 ','
               ),
               ''   --No rows found - deliver empty fc
             )
             || '] }' AS featureCollection
            """;
    }

    return "SELECT " + selection + " FROM (" + innerSelect + ") t";
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

  private void batchInsertFeatures(
          String schema,
          String table,
          FeatureCollection featureCollection,
          long version,
          String author,
          List<ModificationFailure> fails
  ) throws SQLException, JsonProcessingException {

    String insertSql = """
        INSERT INTO %s.%s
          (id, geo, operation, author, version, jsondata, searchable)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """.formatted(schema, table);

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

  private String getRefQuad(Feature feature) {
    return feature.getProperties().get(REF_QUAD_PROPERTY_KEY);
  }

  private Integer getGlobalVersion(Feature feature) {
    Integer globalVersion = feature.getProperties().get(GLOBAL_VERSION_PROPERTY_KEY);
    return globalVersion == null ? -1 : globalVersion;
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
}
