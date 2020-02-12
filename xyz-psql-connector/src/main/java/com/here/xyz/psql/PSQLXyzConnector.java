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
import com.here.xyz.events.*;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.responses.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

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
              this::getStatisticsResultSetHandler);

    } catch (SQLException e) {
      throw new SQLException(e);
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    final List<String> ids = event.getIds();
    if (ids == null || ids.size() == 0) {
      return new FeatureCollection();
    }
    return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByIdQuery(event, config, dataSource));
  }

  @Override
  protected XyzResponse processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByGeometryQuery(event,dataSource));
  }

  @Override
  protected XyzResponse processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    final BBox bbox = event.getBbox();
    final String clusteringType = event.getClusteringType();
    final Map<String, Object> clusteringParams = event.getClusteringParams();

    if (clusteringType != null && H3SQL.HEXBIN.equalsIgnoreCase(clusteringType)) {
      return executeQueryWithRetry(SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, clusteringParams,dataSource));
    } else if (clusteringType != null && QuadbinSQL.QUAD.equalsIgnoreCase(clusteringType)) {
      /** Check if input is valid */
      final int resolution = clusteringParams.get("resolution") != null ? (int) clusteringParams.get("resolution") : 0;
      final String quadMode = clusteringParams.get("quadmode") != null ? (String) clusteringParams.get("quadmode") : null;
      QuadbinSQL.checkQuadbinInput(quadMode, resolution, event, streamId, this);
      return executeQueryWithRetry(SQLQueryBuilder.buildQuadbinClusteringQuery(event, bbox, resolution, quadMode, config));
    }

    final boolean isBigQuery = (bbox.widthInDegree(false) >= (360d / 4d) || (bbox.heightInDegree() >= (180d / 4d)));

    if(isBigQuery){
      /** Check if Properties are indexed */
      if (!Capabilities.canSearchFor(event.getSpace(), event.getPropertiesQuery(), this)) {
        throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                "Invalid request parameters. Search for the provided properties is not supported for this space.");
      }
    }
    return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event, isBigQuery, dataSource));
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
              this::countResultSetHandler);
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
    if (config.isReadOnly()) {
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.NOT_IMPLEMENTED)
              .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
    }
    boolean includeOldStates = event.getParams() != null
            && event.getParams().get(PSQLConfig.INCLUDE_OLD_STATES) == Boolean.TRUE;

    final SQLQuery searchQuery = SQLQueryBuilder.generateSearchQuery(event, dataSource);
    final SQLQuery query = SQLQueryBuilder.buildDeleteFeaturesByTagQuery(event, includeOldStates, searchQuery, dataSource);

    //TODO: check in detail what we want to return
    if (searchQuery != null && includeOldStates)
      return executeUpdateWithRetry(query, this::oldStatesResultSetHandler);

    return new FeatureCollection().withCount((long) executeUpdateWithRetry(query));
  }

  @Override
  protected XyzResponse processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    final Map<String, String> idMap = event.getIdsMap();
    if (idMap == null || idMap.size() == 0) {
      return new FeatureCollection();
    }
    return executeQueryWithRetry(SQLQueryBuilder.buildLoadFeaturesQuery(event,idMap,dataSource));
  }

  @Override
  protected XyzResponse processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    if (config.isReadOnly()) {
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.NOT_IMPLEMENTED)
              .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
    }

    final long now = System.currentTimeMillis();
    final boolean addUUID = event.getEnableUUID() == Boolean.TRUE && event.getVersion().compareTo("0.2.0") < 0;
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

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {

    if ((ModifySpaceEvent.Operation.UPDATE == event.getOperation()
            || ModifySpaceEvent.Operation.CREATE == event.getOperation())
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
      executeUpdateWithRetry(  SQLQueryBuilder.buildSearchablePropertiesUpsertQuery(
              event.getSpaceDefinition().getSearchableProperties(),
              event.getOperation(),
              config.schema(), config.table(event)));
    }

    if (ModifySpaceEvent.Operation.DELETE == event.getOperation()) {
      if (hasTable()) {
        try (final Connection connection = dataSource.getConnection()) {
          try (Statement stmt = connection.createStatement()) {
            String query = "DROP TABLE ${schema}.${table}";
            query = SQLQuery.replaceVars(query, config.schema(), config.table(event));
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


  protected XyzResponse findFeatures(SearchForFeaturesEvent event, final String handle, final boolean isIterate)
          throws Exception{
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
  }
}