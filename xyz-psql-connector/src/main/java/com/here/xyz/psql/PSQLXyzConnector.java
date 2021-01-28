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
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.WebMercatorTile;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
      final List<String> ids = event.getIds();
      if (ids == null || ids.size() == 0) {
        return new FeatureCollection();
      }
      return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByIdQuery(event, config, dataSource));
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished GetFeaturesByIdEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    try {
      logger.info("{} Received GetFeaturesByGeometryEvent", traceItem);
      return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByGeometryQuery(event,dataSource));
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished GetFeaturesByGeometryEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    try{
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);

      final BBox bbox = event.getBbox();

      boolean bTweaks = ( event.getTweakType() != null ),
              bOptViz = "viz".equals( event.getOptimizationMode() ),
              bSelectionStar = false,
              bClustering = (event.getClusteringType() != null);

      int mvtFromDbRequested = SQLQueryBuilder.mvtFromDbRequested(event),
          mvtMargin = 0;
      boolean bMvtFlattend = ( mvtFromDbRequested > 1 ),
              bMvtFromHub  = SQLQueryBuilder.mvtFromHubRequested(event);

      WebMercatorTile mvtTile = null;

      if( mvtFromDbRequested > 0 )
      { GetFeaturesByTileEvent e = (GetFeaturesByTileEvent) event; // TileEvent is garanteed
        mvtTile = WebMercatorTile.forWeb(e.getLevel(), e.getX(), e.getY());
        mvtMargin = e.getMargin();
      }

      if( event.getSelection() != null && "*".equals( event.getSelection().get(0) ))
      { event.setSelection(null);
        bSelectionStar = true; // differentiation needed, due to different semantic of "event.getSelection() == null" tweaks vs. nonTweaks
      }

      if( !bClustering && ( bTweaks || bOptViz || bMvtFromHub ) )
      {
        Map<String, Object> tweakParams;
        boolean bVizSamplingOff = false;

        if( bTweaks )
         tweakParams = event.getTweakParams();
        else if ( bMvtFromHub && !bOptViz )
        { event.setTweakType( TweaksSQL.SIMPLIFICATION );
          tweakParams = new HashMap<String, Object>();
          tweakParams.put("algorithm", new String("gridbytilelevel"));
        }
        else
        { event.setTweakType( TweaksSQL.ENSURE );
          tweakParams = new HashMap<String, Object>();
          switch( event.getVizSampling().toLowerCase() )
          { case "high" : tweakParams.put(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, new Integer( 15 ) ); break;
            case "low"  : tweakParams.put(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, new Integer( 70 ) ); break;    
            case "off"  : tweakParams.put(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, new Integer( 100 )); 
                          bVizSamplingOff = true;
                          break;    
            case "med"  : 
            default     : tweakParams.put(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD, new Integer( 30 ) ); break;
          }
        }

        int distStrength = 0;

        switch ( event.getTweakType().toLowerCase() )  {

          case TweaksSQL.ENSURE: {
            int rCount = executeQueryWithRetry(SQLQueryBuilder.buildEstimateSamplingStrengthQuery(event, bbox )).getFeatures().get(0).get("rcount");

            boolean bDefaultSelectionHandling = (tweakParams.get(TweaksSQL.ENSURE_DEFAULT_SELECTION) == Boolean.TRUE );

            if( event.getSelection() == null && !bDefaultSelectionHandling && !bSelectionStar )
             event.setSelection(Arrays.asList("id","type"));
                        
            distStrength = TweaksSQL.calculateDistributionStrength( rCount, Math.max(Math.min((int) tweakParams.getOrDefault(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD,10),100),10) * 1000 );

            HashMap<String, Object> hmap = new HashMap<String, Object>();    
            hmap.put("algorithm", new String("distribution"));
            hmap.put("strength", new Integer( distStrength ));
            tweakParams = hmap;
            // fall thru tweaks=sampling
          }

          case TweaksSQL.SAMPLING: {
            if( bTweaks || !bVizSamplingOff )
            {
              if( mvtFromDbRequested == 0 )
              { FeatureCollection collection = executeQueryWithRetrySkipIfGeomIsNull(SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, tweakParams, dataSource));
                collection.setPartial(true);
                return collection;
              }
              else
               return executeBinQueryWithRetry(
                         SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, tweakParams, dataSource) , mvtTile, mvtMargin, bMvtFlattend ) );
            }
            else
            { // fall thru tweaks=simplification e.g. mode=viz and vizSampling=off
              tweakParams.put("algorithm", new String("gridbytilelevel"));
            }
          }            
          
          case TweaksSQL.SIMPLIFICATION: { 
            if( mvtFromDbRequested == 0 )
            { FeatureCollection collection = executeQueryWithRetrySkipIfGeomIsNull(SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, tweakParams, dataSource));
              collection.setPartial(true);
              return collection;
            }
            else
             return executeBinQueryWithRetry(
               SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, tweakParams, dataSource) , mvtTile, mvtMargin, bMvtFlattend ) );
          }

          default: break; // fall back to non-tweaks usage.
        }
      }

      if( bClustering )
      { final Map<String, Object> clusteringParams = event.getClusteringParams();

        switch(event.getClusteringType().toLowerCase())
        {
          case H3SQL.HEXBIN :
           if( mvtFromDbRequested == 0 )
            return executeQueryWithRetry(SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, clusteringParams,dataSource));
           else
            return executeBinQueryWithRetry(
             SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, clusteringParams,dataSource), mvtTile, mvtMargin, bMvtFlattend ) );

          case QuadbinSQL.QUAD :
           final int relResolution = ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) :
                                     ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) : 0)),
                     absResolution = clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) : 0;
           final String countMode = (String) clusteringParams.get(QuadbinSQL.QUADBIN_COUNTMODE);
           final boolean noBuffer = (boolean) clusteringParams.getOrDefault(QuadbinSQL.QUADBIN_NOBOFFER,false);

           QuadbinSQL.checkQuadbinInput(countMode, relResolution, event, config.readTableFromEvent(event), streamId, this);

            if( mvtFromDbRequested == 0 )
              return executeQueryWithRetry(SQLQueryBuilder.buildQuadbinClusteringQuery(event, bbox, relResolution, absResolution, countMode, config, noBuffer));
            else
              return executeBinQueryWithRetry(
                      SQLQueryBuilder.buildMvtEncapsuledQuery(config.readTableFromEvent(event), SQLQueryBuilder.buildQuadbinClusteringQuery(event, bbox, relResolution, absResolution, countMode, config, noBuffer), mvtTile, mvtMargin, bMvtFlattend ) );

          default: break; // fall back to non-tweaks usage.
       }
      }

      final boolean isBigQuery = (bbox.widthInDegree(false) >= (360d / 4d) || (bbox.heightInDegree() >= (180d / 4d)));

      if(isBigQuery){
        /* Check if Properties are indexed */
        if (!Capabilities.canSearchFor(config.readTableFromEvent(event), event.getPropertiesQuery(), this)) {
          throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
                  "Invalid request parameters. Search for the provided properties is not supported for this space.");
        }
      }

      if( mvtFromDbRequested == 0 )
       return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event, isBigQuery, dataSource));
      else
       return executeBinQueryWithRetry( SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event, isBigQuery, dataSource), mvtTile, mvtMargin, bMvtFlattend ) );

    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished "+event.getClass().getSimpleName(), traceItem);
    }
  }

  @Override
  protected XyzResponse processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    return processGetFeaturesByBBoxEvent(event);
  }

  @Override
  protected XyzResponse processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    if(event.getV() != null)
      return iterateVersions(event);
    return findFeatures(event, event.getHandle(), true);
  }

  @Override
  protected XyzResponse processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    return findFeatures(event, null, false);
  }

  @Override
  protected XyzResponse processCountFeaturesEvent(CountFeaturesEvent event) throws Exception {
    try {
      logger.info("{} Received CountFeaturesEvent", traceItem);
      return executeQueryWithRetry(SQLQueryBuilder.buildCountFeaturesQuery(event, dataSource, config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event)),
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
    }finally {
      logger.info("{} Finished CountFeaturesEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processDeleteFeaturesByTagEvent(DeleteFeaturesByTagEvent event) throws Exception {
    try{
      logger.info("{} Received DeleteFeaturesByTagEvent", traceItem);
      if (config.getDatabaseSettings().isReadOnly()) {
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.NOT_IMPLEMENTED)
                .withErrorMessage("ModifyFeaturesEvent is not supported by this storage connector.");
      }
      return executeDeleteFeaturesByTag(event);
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished DeleteFeaturesByTagEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    try{
      logger.info("{} Received LoadFeaturesEvent", traceItem);
      return executeLoadFeatures(event);
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
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished ModifyFeaturesEvent", traceItem);
    }
  }

  @Override
  protected XyzResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    try{
      logger.info("{} Received ModifySpaceEvent", traceItem);

      if(event.getSpaceDefinition() != null && event.getSpaceDefinition().isEnableUUID()){
        if(event.getSpaceDefinition().isEnableHistory()){
          Integer maxVersionCount = event.getSpaceDefinition().getMaxVersionCount();
          Boolean isEnableGlobalVersioning = event.getSpaceDefinition().isEnableGlobalVersioning();
          Boolean compactHistory = config.getConnectorParams().isCompactHistory();

          if(ModifySpaceEvent.Operation.CREATE == event.getOperation()){
            ensureHistorySpace(maxVersionCount, compactHistory, isEnableGlobalVersioning);
          }else if(ModifySpaceEvent.Operation.UPDATE == event.getOperation()){
            //update Trigger to apply maxVersionCount.
            updateTrigger(maxVersionCount, compactHistory, isEnableGlobalVersioning);
          }
        }
      }

      if ((ModifySpaceEvent.Operation.UPDATE == event.getOperation()
              || ModifySpaceEvent.Operation.CREATE == event.getOperation())
              && config.getConnectorParams().isPropertySearch()) {

        if (event.getSpaceDefinition().getSearchableProperties() != null) {
          int onDemandLimit = config.getConnectorParams().getOnDemandIdxLimit();

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
                  config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event)));
      }

      if (ModifySpaceEvent.Operation.DELETE == event.getOperation()) {
        boolean hasTable = hasTable();

        if (hasTable) {
          SQLQuery q = new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};");
          q.append("DROP TABLE IF EXISTS ${schema}.${hsttable};");
          q.append("DROP SEQUENCE IF EXISTS "+ config.getDatabaseSettings().getSchema()+".\""+config.readTableFromEvent(event).replaceAll("-","_")+"_serial\";");
          q.append("DROP SEQUENCE IF EXISTS " +config.getDatabaseSettings().getSchema() + ".\"" +config.readTableFromEvent(event).replaceAll("-", "_") + "_hst_seq\";");

          executeUpdateWithRetry(q);
          logger.debug("{} Successfully deleted table '{}' for space id '{}'", traceItem, config.readTableFromEvent(event), event.getSpace());
        } else
          logger.debug("{} Table '{}' not found for space id '{}'", traceItem, config.readTableFromEvent(event), event.getSpace());

        if (event.getConnectorParams() != null && event.getConnectorParams().get("propertySearch") == Boolean.TRUE) {
          executeUpdateWithRetry(SQLQueryBuilder.buildDeleteIDXConfigEntryQuery(config.getDatabaseSettings().getSchema(),config.readTableFromEvent(event)));
        }
      }
      return new SuccessResponse().withStatus("OK");
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished ModifySpaceEvent", traceItem);
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

  protected XyzResponse iterateVersions(IterateFeaturesEvent event){
    try{
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);
      return executeIterateVersions(event);
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished "+event.getClass().getSimpleName(), traceItem);
    }
  }

  protected XyzResponse findFeatures(SearchForFeaturesEvent event, final String handle, final boolean isIterate)
          throws Exception{
    try{
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);

      final SQLQuery searchQuery = SQLQueryBuilder.generateSearchQuery(event,dataSource);
      final boolean hasSearch = searchQuery != null;
      final boolean hasHandle = handle != null;
      final long start = hasHandle ? Long.parseLong(handle) : 0L;

      // For testing purposes.
      if (event.getSpace().equals("illegal_argument")) {
        return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
                .withErrorMessage("Invalid request parameters.");
      }

      if (!Capabilities.canSearchFor(config.readTableFromEvent(event), event.getPropertiesQuery(), this)) {
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
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished "+event.getClass().getSimpleName(), traceItem);
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
                                .withErrorMessage(String.format("clustering.property: string(%s) can not be converted to numeric",( m.find() ? m.group(1) : "" )));
     }

     case "22P05" :
     {
      if( e.getMessage() == null ) break;
      String eMsg = "untranslatable character in payload";
      Matcher m = ERRVALUE_22P05.matcher(e.getMessage());

      if( m.find() )
       eMsg = String.format( eMsg + ": %s - %s",m.group(1), m.group(2));

      return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT).withErrorMessage( eMsg );
     }

     case "42P01" :
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.TIMEOUT).withErrorMessage(e.getMessage());

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

    return new ErrorResponse().withStreamId(streamId).withError(XyzError.EXCEPTION).withErrorMessage(e.getMessage());
  }
}
