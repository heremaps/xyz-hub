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
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.CountFeaturesEvent;
import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.responses.CountResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PSQLXyzConnector extends DatabaseHandler {

  private static final Logger logger = LogManager.getLogger();
  /**
   * The context for this request.
   */
  @SuppressWarnings("WeakerAccess")
  protected Context context;

  @Override
  protected XyzResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    try {
      return executeQueryWithRetry(SQLQueryBuilder.buildGetStatisticsQuery(event,config),
              this::getStatisticsResultSetHandler, true);
    }catch (SQLException e){
      return checkSQLException(e, config.table(event));
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    try {
      final List<String> ids = event.getIds();
      if (ids == null || ids.size() == 0) {
        return new FeatureCollection();
      }
      return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByIdQuery(event, config, dataSource));
    }catch (SQLException e){
      return checkSQLException(e, config.table(event));
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    try {
      return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByGeometryQuery(event,dataSource));
    }catch (SQLException e){
      return checkSQLException(e, config.table(event));
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    try{
      final BBox bbox = event.getBbox();
      final String clusteringType = event.getClusteringType();
      final Map<String, Object> clusteringParams = event.getClusteringParams();

      if(event.getTweakType() != null) {
        switch (event.getTweakType().toLowerCase()) {

          case TweaksSQL.SAMPLING:

            FeatureCollection collection = executeQueryWithRetry(SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, event.getTweakParams(), dataSource));
            collection.setPartial(true);
            return collection;

          case TweaksSQL.SIMPLIFICATION:

            FeatureCollection fcollection = executeQueryWithRetry(SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, event.getTweakParams(), dataSource));
            fcollection.setPartial(true);
            return fcollection;

          default: break; // fall back to non-tweaks usage.
        }
      }

      if (H3SQL.HEXBIN.equalsIgnoreCase(clusteringType)) {
        return executeQueryWithRetry(SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, clusteringParams,dataSource));
      } else if ( QuadbinSQL.QUAD.equalsIgnoreCase(clusteringType)) {
        /* Check if input is valid */
        final int relResolution = ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) :
                                  ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) : 0)),
                  absResolution = clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) : 0;
        final String countMode = (String) clusteringParams.get(QuadbinSQL.QUADBIN_COUNTMODE);

        QuadbinSQL.checkQuadbinInput(countMode, relResolution, event, streamId, this);
        return executeQueryWithRetry(SQLQueryBuilder.buildQuadbinClusteringQuery(event, bbox, relResolution, absResolution, countMode, config));
      }

      final boolean isBigQuery = (bbox.widthInDegree(false) >= (360d / 4d) || (bbox.heightInDegree() >= (180d / 4d)));

      if(isBigQuery){
        /* Check if Properties are indexed */
        if (!Capabilities.canSearchFor(event.getSpace(), event.getPropertiesQuery(), this)) {
          throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                  "Invalid request parameters. Search for the provided properties is not supported for this space.");
        }
      }
      return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event, isBigQuery, dataSource));
    }catch (SQLException e){
      return checkSQLException(e, config.table(event));
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    return processGetFeaturesByBBoxEvent(event);
  }

  @Override
  protected XyzResponse processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    return findFeatures(event, event.getHandle(), true);
  }

  @Override
  protected XyzResponse processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    return findFeatures(event, null, false);
  }

  @Override
  protected XyzResponse processCountFeaturesEvent(CountFeaturesEvent event) throws Exception {
    try {
      return executeQueryWithRetry(SQLQueryBuilder.buildCountFeaturesQuery(event, dataSource, config.schema(), config.table(event)),
              this::countResultSetHandler, true);
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

  @Override
  protected XyzResponse processDeleteFeaturesByTagEvent(DeleteFeaturesByTagEvent event) throws Exception {
    try{
      if (config.isReadOnly()) {
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.NOT_IMPLEMENTED)
                .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
      }
      return executeDeleteFeaturesByTag(event);
    }catch (SQLException e){
      return checkSQLException(e, config.table(event));
    }
  }

  @Override
  protected XyzResponse processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    try{
      return executeLoadFeatures(event);
    }catch (SQLException e){
      return checkSQLException(e, config.table(event));
    }
  }

  @Override
  protected XyzResponse processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    try{
      if (config.isReadOnly()) {
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.NOT_IMPLEMENTED)
                .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
      }

      final boolean addUUID = event.getEnableUUID() == Boolean.TRUE && event.getVersion().compareTo("0.2.0") < 0;
      // Update the features to insert
      final List<Feature> inserts = Optional.ofNullable(event.getInsertFeatures()).orElse(Collections.emptyList());
      final List<Feature> updates = Optional.ofNullable(event.getUpdateFeatures()).orElse(Collections.emptyList());
      final List<Feature> upserts = Optional.ofNullable(event.getUpsertFeatures()).orElse(Collections.emptyList());

      // Generate feature ID
      Stream.of(inserts, upserts)
          .flatMap(Collection::stream)
          .filter(feature -> feature.getId() == null)
          .forEach(feature -> feature.setId(RandomStringUtils.randomAlphanumeric(16)));

      // Call finalize feature
      Stream.of(inserts, updates, upserts)
          .flatMap(Collection::stream)
          .forEach(feature -> Feature.finalizeFeature(feature, event.getSpace(), addUUID));

      return executeModifyFeatures(event);
    } catch (SQLException e) {
      return checkSQLException(e, config.table(event));
    }
  }

  @Override
  protected XyzResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    try{
      if(event.getSpaceDefinition() != null && event.getSpaceDefinition().isEnableUUID()){
        Integer maxVersionCount = null;

        if(event.getParams() != null)
          maxVersionCount = (Integer)event.getParams().get("maxVersionCount");

        if(event.getSpaceDefinition().isEnableHistory()){
          if(ModifySpaceEvent.Operation.CREATE == event.getOperation()){
            ensureHistorySpace(maxVersionCount);
          }else if(ModifySpaceEvent.Operation.UPDATE == event.getOperation()){
            //TODO: ONLY update Trigger
            ensureHistorySpace(maxVersionCount);
          }
        }
      }

      if ((ModifySpaceEvent.Operation.UPDATE == event.getOperation()
              || ModifySpaceEvent.Operation.CREATE == event.getOperation())
              && event.getConnectorParams() != null
              && event.getConnectorParams().get("propertySearch") == Boolean.TRUE) {

        if (event.getSpaceDefinition().getSearchableProperties() != null) {
          int onDemandLimit = (event.getConnectorParams() != null && event.getConnectorParams().get("onDemandIdxLimit") != null) ?
                  (Integer) event.getConnectorParams().get("onDemandIdxLimit") : DatabaseMaintainer.ON_DEMAND_IDX_DEFAULT_LIM;

          int cnt = 0;
          for (String property : event.getSpaceDefinition().getSearchableProperties().keySet()) {
            if (event.getSpaceDefinition().getSearchableProperties().get(property) != null
                    && event.getSpaceDefinition().getSearchableProperties().get(property) == Boolean.TRUE) {
              cnt++;
            }
            if (cnt > onDemandLimit) {
              throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                      "On-Demand-Indexing - Maximum permissible: " + onDemandLimit + " searchable properties per space!");
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

        //TODO: Check if config entry exists and idx_manual=null -> update it (erase on demand)
        if(!(ModifySpaceEvent.Operation.CREATE == event.getOperation() && event.getSpaceDefinition().getSearchableProperties() == null))
          executeUpdateWithRetry(  SQLQueryBuilder.buildSearchablePropertiesUpsertQuery(
                  event.getSpaceDefinition().getSearchableProperties(),
                  event.getOperation(),
                  config.schema(), config.table(event)));
      }

      if (ModifySpaceEvent.Operation.DELETE == event.getOperation()) {
        boolean hasTable = hasTable();

        if (hasTable) {
          SQLQuery q = new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};");
          q.append("DROP TABLE IF EXISTS ${schema}.${hsttable}");
          executeUpdateWithRetry(q);
          logger.info("{} - Successfully deleted table for space '{}'", streamId, event.getSpace());
        } else
          logger.info("{} - Table not found '{}'", streamId, event.getSpace());

        if (event.getConnectorParams() != null && event.getConnectorParams().get("propertySearch") == Boolean.TRUE) {
          executeUpdateWithRetry(SQLQueryBuilder.buildDeleteIDXConfigEntryQuery(config.schema(),config.table(event)));
        }
      }
      return new SuccessResponse().withStatus("OK");
    }catch (SQLException e){
      return checkSQLException(e, config.table(event));
    }
  }

  protected XyzResponse findFeatures(SearchForFeaturesEvent event, final String handle, final boolean isIterate)
          throws Exception{
    try{
      final SQLQuery searchQuery = SQLQueryBuilder.generateSearchQuery(event,dataSource);
      final boolean hasSearch = searchQuery != null;
      final boolean hasHandle = handle != null;
      final long start = hasHandle ? Long.parseLong(handle) : 0L;

      // For testing purposes.
      if (event.getSpace().equals("illegal_argument")) {
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
                .withErrorMessage("Invalid request parameters.");
      }

      if (!Capabilities.canSearchFor(event.getSpace(), event.getPropertiesQuery(), this)) {
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
                .withErrorMessage("Invalid request parameters. Search for the provided properties is not supported for this space.");
      }

      SQLQuery query = SQLQueryBuilder.buildFeaturesQuery(event, isIterate, hasHandle, hasSearch, start, dataSource) ;

      FeatureCollection collection = executeQueryWithRetry(query);
      if (isIterate && hasSearch && collection.getHandle() != null) {
        collection.setHandle("" + (start + event.getLimit()));
      }

      return collection;
    }catch (SQLException e){
      return checkSQLException(e, config.table(event));
    }
  }

  private static final Pattern ERRVALUE_22P02 = Pattern.compile("invalid input syntax for type numeric:\\s+\"([^\"]*)\"\\s+Query:");

  protected XyzResponse checkSQLException(SQLException e, String table) throws Exception{
    logger.warn("{} - SQL Error ({}) on {} : {}", streamId, e.getSQLState(), table, e);

    String sqlState = ( e.getSQLState() != null ? e.getSQLState().toUpperCase() : "SNULL" );

    switch( sqlState ) 
    {   
     case "57014" : /* 57014 - query_canceled */
     case "57P01" : /* 57P01 - admin_shutdown */
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.TIMEOUT)
                                .withErrorMessage("Database query timed out or got canceled.");
      
     case "54000" :
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.TIMEOUT)
                                .withErrorMessage("No time for retry left for database query.");

     case "22P02" : // specific handling in case to H3 clustering.property
      if( e.getMessage() == null || e.getMessage().indexOf("'H3'::text") == -1 ) break;
      
      Matcher m = ERRVALUE_22P02.matcher(e.getMessage());
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
                                .withErrorMessage(String.format("clustering.property: string(%s) can not be converted to numeric",( m.find() ? m.group(1) : "" ))); 

     case "SNULL" :
      if(e.getMessage() == null || e.getMessage().indexOf("An attempt by a client to checkout a Connection has timed out.") == -1) break;

      return new ErrorResponse().withStreamId(streamId).withError(XyzError.TIMEOUT)
                                 .withErrorMessage("Cant get a Connection to the database.");

     default: break;
    }

    return new ErrorResponse().withStreamId(streamId).withError(XyzError.EXCEPTION).withErrorMessage(e.getMessage());
  }
}
