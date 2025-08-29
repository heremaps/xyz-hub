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
import com.here.xyz.connectors.ErrorResponseException;
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
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.FeatureCollection.ModificationFailure;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.query.GetFastStatistics;
import com.here.xyz.psql.query.GetFeaturesByBBox;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.psql.query.GetFeaturesById;
import com.here.xyz.psql.query.IterateFeatures;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.psql.query.helpers.versioning.GetNextVersion;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.io.WKBWriter;


import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;
import static com.here.xyz.events.UpdateStrategy.OnExists;
import static com.here.xyz.events.UpdateStrategy.OnNotExists;

import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;

public class NLConnector extends PSQLXyzConnector {
  private static final Logger logger = LogManager.getLogger();

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
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected BinaryResponse processBinaryGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    throw new ErrorResponseException(NOT_IMPLEMENTED, "Method not implemented in NLConnector!");
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
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
    Set<WriteFeaturesEvent.Modification> modifications = event.getModifications();

    /*
     * INSERT/UPDATE: UpdateStrategy[onExists=REPLACE, onNotExists=CREATE, onVersionConflict=null, onMergeConflict=null]
     * DELETE: UpdateStrategy[onExists=DELETE, onNotExists=RETAIN, onVersionConflict=null, onMergeConflict=null]
     *
     * simple batch Upsert
     * enrich: update TimeStamp, Version
     *
     **/

    List<String> idsToDelete = new ArrayList<>();
    FeatureCollection upsertFeatures = new FeatureCollection();
    FeatureCollection responseFeatureCollection = new FeatureCollection();

    for (WriteFeaturesEvent.Modification modification : modifications) {
      validateUpdateStrategy(modification.getUpdateStrategy());

      if(modification.getUpdateStrategy().onExists().equals(OnExists.DELETE)) {
        //Batch delete
        if(modification.getFeatureIds() == null || modification.getFeatureIds().isEmpty()){
          modification.getFeatureData().getFeatures().forEach(f -> idsToDelete.add(f.getId()));
        }else
          idsToDelete.addAll(modification.getFeatureIds());
      }
      else if(modification.getUpdateStrategy().onExists().equals(OnExists.REPLACE)
          && modification.getUpdateStrategy().onNotExists().equals(OnNotExists.CREATE)) {
        //Batch upsert
        upsertFeatures.getFeatures().addAll(modification.getFeatureData().getFeatures());
      }
    }

    List<ModificationFailure> fails = new ArrayList<>();

    if(!idsToDelete.isEmpty()) {
      batchDeleteFeatures(dbSettings.getSchema(), XyzEventBasedQueryRunner.readTableFromEvent(event), idsToDelete, fails);
      //TODO: Add deletedIds to responseFeatureCollection?
    }

    if(!upsertFeatures.getFeatures().isEmpty()) {
      //retrieve Version
      Long version = run(new GetNextVersion<>(event));
      batchUpsertFeatures(dbSettings.getSchema(), XyzEventBasedQueryRunner.readTableFromEvent(event), upsertFeatures, version, event.getAuthor(), fails);
      //TODO: Add upsertedIds to responseFeatureCollection?
    }

    if(!fails.isEmpty())
      return new FeatureCollection().withFailed(fails);
    return new FeatureCollection().withDeleted(List.of(idsToDelete.toArray(new String[0])));
  }

  private void validateUpdateStrategy(UpdateStrategy updateStrategy) throws ErrorResponseException {
    List<UpdateStrategy.OnExists> supportedOnExistsStrategies = List.of(UpdateStrategy.OnExists.REPLACE, UpdateStrategy.OnExists.DELETE);
    List<UpdateStrategy.OnNotExists> supportedOnNotExistsStrategies = List.of(UpdateStrategy.OnNotExists.CREATE, UpdateStrategy.OnNotExists.RETAIN);

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

  private void batchUpsertFeatures(String schema, String table, FeatureCollection featureCollection, long version, String author,
          List<ModificationFailure> fails) throws SQLException, JsonProcessingException {
//    String upsertSql = """
//          INSERT INTO $table$ (id, jsondata, operation, version, geo)
//          VALUES (
//             ?,
//             regexp_replace(
//               -- remove geometry
//               regexp_replace(
//                 -- override timestamps if NS present
//                 regexp_replace(
//                   -- add NS if not present
//                   ?,
//                   E'"@ns:com:here:xyz":\\\\{[^}]*\\\\}',
//                   '"@ns:com:here:xyz":{"createdAt":' || (extract(epoch from now())*1000)::bigint ||
//                   ',"updatedAt":' || (extract(epoch from now())*1000)::bigint || '}',
//                   'g'
//                 ),
//                 E'"properties":\\\\{((?!@ns:com:here:xyz).)*\\\\}',
//                 E'"properties":{' ||
//                   '"@ns:com:here:xyz":{"createdAt":' || (extract(epoch from now())*1000)::bigint ||
//                   ',"updatedAt":' || (extract(epoch from now())*1000)::bigint || '},',
//                 'g'
//               ),
//               E'"geometry":\\\\{[^}]*\\\\},?',
//               '',
//               'g'
//             ),
//             'I',
//             ?,
//             ?
//           )
//          ON CONFLICT (id, next_version) DO UPDATE
//            SET jsondata =
//              -- replace createdAt with existing value, updatedAt with now()
//              regexp_replace(
//                regexp_replace(
//                  EXCLUDED.jsondata,
//                  E'("@ns:com:here:xyz":\\\\{[^}]*"createdAt":)[0-9]+',
//                  E'\\\\1' || substring(
//                    $table$.jsondata
//                      FROM E'"@ns:com:here:xyz":\\\\{[^}]*"createdAt":([0-9]+)'
//                  )
//                ),
//                E'("updatedAt":)[0-9]+',
//                E'\\\\1' || extract(epoch from now())::bigint
//              ),
//              geo = EXCLUDED.geo,
//              version = EXCLUDED.version,
//              operation = 'U';
//    """.replace("$table$", "\"" + schema + "\".\"" + table + "\"");
    String upsertSql = """
          INSERT INTO $table$ (id, geo, operation, author, version, jsondata)
          VALUES (
             ?,
             ?,
             'I',
             ?,
             ?,
             ?
           )
          ON CONFLICT (id, next_version) DO UPDATE
            SET jsondata = EXCLUDED.jsondata,
              geo = EXCLUDED.geo,
              version = EXCLUDED.version,
              author = EXCLUDED.author,
              operation = 'U';
    """.replace("$table$", "\"" + schema + "\".\"" + table + "\"");

    try (Connection connection = dataSourceProvider.getWriter().getConnection()) {
      connection.setAutoCommit(false);

      try (PreparedStatement ps = connection.prepareStatement(upsertSql)) {
        for (Feature feature : featureCollection.getFeatures()) {
          ensureFeatureId(feature);

          ps.setString(1, feature.getId());
          ps.setBytes(2, new WKBWriter(3).write(feature.getGeometry().getJTSGeometry()));  // JSON string
          ps.setString(3, author);
          ps.setLong(4, version);
          //TODO: Do we need to ensure 3D geometries?
          ps.setString(5, enrichFeaturePayload(feature));
          ps.addBatch();
        }

        int[] fcCount = ps.executeBatch();
        connection.commit();
        logger.info("Successfully upserted {} features.", fcCount.length);
      } catch (SQLException e) {
        connection.rollback();
        logger.error("Upsert failed", e);
        fails.add(new ModificationFailure().withMessage("Upsert failed: " + e.getMessage()));
      }
    }
  }

  private void ensureFeatureId(Feature feature) {
    if(feature.getId() == null){
      feature.setId(Random.randomAlphaNumeric(16));
    }
  }

  private String enrichFeaturePayload(Feature feature) {
    //LFE is missing - so we do not have the createdAt from db | also patch possibility is missing
    //Ensure 3D geometry

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
