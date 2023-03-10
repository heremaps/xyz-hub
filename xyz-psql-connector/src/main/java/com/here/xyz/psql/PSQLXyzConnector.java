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

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT_FLATTENED;
import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.query.DeleteChangesets;
import com.here.xyz.psql.query.GetFeaturesByBBox;
import com.here.xyz.psql.query.GetFeaturesByBBoxClustered;
import com.here.xyz.psql.query.GetFeaturesByBBoxTweaked;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.psql.query.GetFeaturesById;
import com.here.xyz.psql.query.GetChangesetStatistics;
import com.here.xyz.psql.query.GetStorageStatistics;
import com.here.xyz.psql.query.IterateChangesets;
import com.here.xyz.psql.query.IterateFeatures;
import com.here.xyz.psql.query.LoadFeatures;
import com.here.xyz.psql.query.SearchForFeatures;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PSQLXyzConnector extends DatabaseHandler {

  private static final Logger logger = LogManager.getLogger();

  @Override
  protected XyzResponse processHealthCheckEvent(HealthCheckEvent event) {
    try {
      logger.info("{} Received HealthCheckEvent", traceItem);
      return processHealthCheckEventImpl(event);
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished HealthCheckEvent", traceItem);
    }
  }
  @Override
  protected XyzResponse processGetHistoryStatisticsEvent(GetHistoryStatisticsEvent event) throws Exception {
    try {
      logger.info("{} Received HistoryStatisticsEvent", traceItem);
      return executeQueryWithRetry(SQLQueryBuilder.buildGetStatisticsQuery(event, config, true),
                this::getHistoryStatisticsResultSetHandler, true);
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished GetHistoryStatisticsEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    try {
      logger.info("{} Received GetStatisticsEvent", traceItem);
      return executeQueryWithRetry(SQLQueryBuilder.buildGetStatisticsQuery(event, config, false),
              this::getStatisticsResultSetHandler, true);
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished GetStatisticsEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    try {
      logger.info("{} Received GetFeaturesByIdEvent", traceItem);
      if (event.getIds() == null || event.getIds().size() == 0)
        return new FeatureCollection();

      return new GetFeaturesById(event, this).run();
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished GetFeaturesByIdEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    try {
      logger.info("{} Received GetFeaturesByGeometryEvent", traceItem);
        return new GetFeaturesByGeometry(event, this).run();
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished GetFeaturesByGeometryEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    try {
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);

      if (event.getClusteringType() != null)
        return new GetFeaturesByBBoxClustered<>(event, this).run();
      if (event.getTweakType() != null || "viz".equals(event.getOptimizationMode()))
        return new GetFeaturesByBBoxTweaked<>(event, this).run();
      return new GetFeaturesByBBox<>(event, this).run();
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished "+event.getClass().getSimpleName(), traceItem);
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    if (event.getHereTileFlag() && event.getClusteringType() != null)
      throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "clustering=[hexbin,quadbin] is not supported for 'here' tile type. Use Web Mercator projection (quadkey, web, tms).");

    if ((event.getResponseType() == MVT || event.getResponseType() == MVT_FLATTENED)
        && (event.getConnectorParams() == null || event.getConnectorParams().get("mvtSupport") != Boolean.TRUE))
      throw new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, "mvt format is not supported");

    return processGetFeaturesByBBoxEvent(event);
  }

  @Override
  protected XyzResponse processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    try {
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);
      SearchForFeatures.checkCanSearchFor(event, this);

      if (isOrderByEvent(event))
        return IterateFeatures.findFeaturesSort(event, this);
      if (event.getV() != null)
        return iterateVersions(event);

      return new IterateFeatures(event, this).run();
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished " + event.getClass().getSimpleName(), traceItem);
    }
  }

  /**
   * Kept for backwards compatibility. Will be removed after refactoring.
   */
  @Deprecated
  public static boolean isOrderByEvent(IterateFeaturesEvent event) {
    return event.getSort() != null || event.getPropertiesQuery() != null || event.getPart() != null || event.getHandle() != null && event.getHandle().startsWith(
        IterateFeatures.HPREFIX);
  }

  @Override
  protected XyzResponse processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    try {
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);
      SearchForFeatures.checkCanSearchFor(event, this);

      // For testing purposes.
      if (event.getSpace().contains("illegal_argument")) //TODO: Remove testing code from the actual connector implementation
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
            .withErrorMessage("Invalid request parameters.");

      return new SearchForFeatures<>(event, this).run();
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished " + event.getClass().getSimpleName(), traceItem);
    }
  }

  @Override
  protected XyzResponse processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    try{
      logger.info("{} Received LoadFeaturesEvent", traceItem);
      if (event.getIdsMap() == null || event.getIdsMap().size() == 0)
        return new FeatureCollection();

      return new LoadFeatures(event, this).run();
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished LoadFeaturesEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    try{
      logger.info("{} Received ModifyFeaturesEvent", traceItem);

      if (config.getDatabaseSettings().isReadOnly()) {
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.NOT_IMPLEMENTED)
                .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
      }

      final boolean addUUID = event.getEnableUUID() && event.getVersion().compareTo("0.2.0") < 0;
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
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished ModifyFeaturesEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    try{
      logger.info("{} Received ModifySpaceEvent", traceItem);

      if (config.getConnectorParams().isIgnoreCreateMse())
        return new SuccessResponse().withStatus("OK");

      this.validateModifySpaceEvent(event);

      return executeModifySpace(event);
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished ModifySpaceEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    try{
      logger.info("{} Received ModifySpaceEvent", traceItem);

      this.validateModifySubscriptionEvent(event);

      return executeModifySubscription(event);
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished ModifySpaceEvent", traceItem);
    }
  }

  private void validateModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {

   switch(event.getOperation())
   { case CREATE : case UPDATE : case DELETE : break;
     default:
      throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "Modify Subscription - Operation (" + event.getOperation() + ") not supported" );
   }

  }

  @Override
  protected XyzResponse processIterateHistoryEvent(IterateHistoryEvent event) {
    logger.info("{} Received IterateHistoryEvent", traceItem);
    try{
      return executeIterateHistory(event);
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished IterateHistoryEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    try {
      logger.info("{} Received " + event.getClass().getSimpleName(), traceItem);
      return new GetStorageStatistics(event, this).run();
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished " + event.getClass().getSimpleName(), traceItem);
    }
  }

  @Override
  protected XyzResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    try {
      logger.info("{} Received " + event.getClass().getSimpleName(), traceItem);
      int write = new DeleteChangesets(event, this).write();
      if(write == 0)
        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Version < '"+event.getRequestedMinVersion()+"' is already deleted!");
      return new SuccessResponse();
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished " + event.getClass().getSimpleName(), traceItem);
    }
  }

  @Override
  protected XyzResponse processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    try {
      logger.info("{} Received IterateChangesetsEvent", traceItem);
      return new IterateChangesets(event, this).run();
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished IterateChangesetsEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    try {
      logger.info("{} Received GetChangesetsStatisticsEvent", traceItem);
      return new GetChangesetStatistics(event,this).run();
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished GetChangesetsStatisticsEvent", traceItem);
    }
  }

  protected XyzResponse iterateVersions(IterateFeaturesEvent event) throws SQLException {
    return executeIterateVersions(event);
  }

  private void validateModifySpaceEvent(ModifySpaceEvent event) throws Exception{
    final boolean connectorSupportsAI = config.getConnectorParams().isAutoIndexing();

    if ((ModifySpaceEvent.Operation.UPDATE == event.getOperation()
            || ModifySpaceEvent.Operation.CREATE == event.getOperation())
            && config.getConnectorParams().isPropertySearch()) {

      int onDemandLimit = config.getConnectorParams().getOnDemandIdxLimit();
      int onDemandCounter = 0;
      if (event.getSpaceDefinition().getSearchableProperties() != null) {

        for (String property : event.getSpaceDefinition().getSearchableProperties().keySet()) {
          if (event.getSpaceDefinition().getSearchableProperties().get(property) != null
                  && event.getSpaceDefinition().getSearchableProperties().get(property) == Boolean.TRUE) {
            onDemandCounter++;
          }
          if ( onDemandCounter > onDemandLimit ) {
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
          if (event.getSpaceDefinition().isEnableAutoSearchableProperties() != null
                 && event.getSpaceDefinition().isEnableAutoSearchableProperties()
                  && !connectorSupportsAI) {
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                    "Connector does not support Auto-indexing!");
          }
        }
      }

      if(event.getSpaceDefinition().getSortableProperties() != null )
      { /* todo: eval #index limits, parameter validation  */
        if( event.getSpaceDefinition().getSortableProperties().size() + onDemandCounter > onDemandLimit )
         throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                 "On-Demand-Indexing - Maximum permissible: " + onDemandLimit + " sortable + searchable properties per space!");

        for( List<Object> l : event.getSpaceDefinition().getSortableProperties() )
         for( Object p : l )
         { String property = p.toString();
           if( property.contains("\\") || property.contains("'") )
            throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
              "On-Demand-Indexing [" + property + "] - Characters ['\\] not allowed!");
         }
      }
    }
  }

  private static final Pattern ERRVALUE_22P02 = Pattern.compile("invalid input syntax for type numeric:\\s+\"([^\"]*)\"\\s+Query:"),
                               ERRVALUE_22P05 = Pattern.compile("ERROR:\\s+(.*)\\s+Detail:\\s+(.*)\\s+Where:");

  protected XyzResponse checkSQLException(SQLException e, String table) {
    logger.warn("{} SQL Error ({}) on {} : {}", traceItem, e.getSQLState(), table, e);

    String sqlState = (e.getSQLState() != null ? e.getSQLState().toUpperCase() : "SNULL");

    switch (sqlState) {
     case "XX000": /* XX000 - internal error */
        if ( e.getMessage() == null ) break;
        if ( e.getMessage().indexOf("interruptedException") != -1 ) break;
        if ( e.getMessage().indexOf("ERROR: stats for") != -1 )
         return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT).withErrorMessage( "statistical data for this space is missing (analyze)" );
        if ( e.getMessage().indexOf("TopologyException") != -1 )
         return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT).withErrorMessage( "geometry with irregular topology (self-intersection, clipping)" );
        if ( e.getMessage().indexOf("ERROR: transform: couldn't project point") != -1 )
         return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT).withErrorMessage( "projection error" );
        if ( e.getMessage().indexOf("ERROR: encode_geometry: 'GeometryCollection'") != -1 )
         return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT).withErrorMessage( "dataset contains invalid geometries" );
        //fall thru - timeout assuming timeout

     case "57014" : /* 57014 - query_canceled */
     case "57P01" : /* 57P01 - admin_shutdown */
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.TIMEOUT)
                                .withErrorMessage("Database query timed out or got canceled.");

     case "54000" :
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.TIMEOUT)
                                .withErrorMessage("No time for retry left for database query.");

     case "22P02" : // specific handling in case to H3 clustering.property
     {
      if( e.getMessage() == null || e.getMessage().indexOf("'H3'::text") == -1 ) break;

      Matcher m = ERRVALUE_22P02.matcher(e.getMessage());
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
                                .withErrorMessage(DhString.format("clustering.property: string(%s) can not be converted to numeric",( m.find() ? m.group(1) : "" )));
     }

     case "22P05" :
     {
      if( e.getMessage() == null ) break;
      String eMsg = "untranslatable character in payload";
      Matcher m = ERRVALUE_22P05.matcher(e.getMessage());

      if( m.find() )
       eMsg = DhString.format( eMsg + ": %s - %s",m.group(1), m.group(2));

      return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT).withErrorMessage( eMsg );
     }

     case "42P01" :
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.TIMEOUT).withErrorMessage(e.getMessage());

     case "40P01" : // Database -> deadlock detected e.g. "Process 9452 waits for ShareLock on transaction 2383228826; blocked by process 9342."
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.CONFLICT).withErrorMessage(e.getMessage());

      case "SNULL":
        if (e.getMessage() == null) break;
      // handle some dedicated messages
      if( e.getMessage().indexOf("An attempt by a client to checkout a connection has timed out.") > -1 )
       return new ErrorResponse().withStreamId(streamId).withError(XyzError.TIMEOUT)
                                 .withErrorMessage("Cannot get a connection to the database.");

       if( e.getMessage().indexOf("Maxchar limit") > -1 )
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.PAYLOAD_TO_LARGE)
                                                         .withErrorMessage("Database result - Maxchar limit exceed");

        break; //others

      default:
        break;
    }

    return new ErrorResponse().withStreamId(streamId).withError(EXCEPTION).withErrorMessage(e.getMessage());
  }
}
