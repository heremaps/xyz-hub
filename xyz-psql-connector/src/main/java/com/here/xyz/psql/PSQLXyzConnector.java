/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.CountFeaturesEvent;
import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesOrderByEvent;
import com.here.xyz.events.TagsQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.responses.CountResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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

  static private class TupleTime 
  { static private Map<String,String> rTuplesMap = new ConcurrentHashMap<String,String>();
    long outTs; 
    TupleTime() { outTs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15); rTuplesMap.clear(); }
    boolean expired() { return  System.currentTimeMillis() > outTs; }
  }

  static private TupleTime ttime = new TupleTime();

  @Override
  protected XyzResponse processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    try{
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);

      final BBox bbox = event.getBbox();

      boolean bTweaks = ( event.getTweakType() != null ),
              bOptViz = "viz".equals( event.getOptimizationMode() ),
              bSelectionStar = false,
              bClustering = (event.getClusteringType() != null);

      int mvtTypeRequested = SQLQueryBuilder.mvtTypeRequested(event),
          mvtMargin = 0;
      boolean bMvtRequested = ( mvtTypeRequested > 0 ),
              bMvtFlattend  = ( mvtTypeRequested > 1 );

      WebMercatorTile mercatorTile = null;
      GetFeaturesByTileEvent tileEv = ( event instanceof GetFeaturesByTileEvent ? (GetFeaturesByTileEvent) event : null );

      if( bClustering && tileEv != null && tileEv.getHereTileFlag() )
       throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "clustering=[hexbin,quadbin] is not supported for 'here' tile type. Use Web Mercator projection (quadkey, web, tms).");

      if( bMvtRequested && tileEv == null )
       throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "mvt format needs tile request");

      if( bMvtRequested )
      { 
        if( event.getConnectorParams() == null || event.getConnectorParams().get("mvtSupport") != Boolean.TRUE )
         throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "mvt format is not supported");
        
        if(!tileEv.getHereTileFlag())
         mercatorTile = WebMercatorTile.forWeb(tileEv.getLevel(), tileEv.getX(), tileEv.getY());

        mvtMargin = tileEv.getMargin();
      }

      if( event.getSelection() != null && "*".equals( event.getSelection().get(0) ))
      { event.setSelection(null);
        bSelectionStar = true; // differentiation needed, due to different semantic of "event.getSelection() == null" tweaks vs. nonTweaks
      }

      if( !bClustering && ( bTweaks || bOptViz ) )
      {
        Map<String, Object> tweakParams;
        boolean bVizSamplingOff = false;

        if( bTweaks )
         tweakParams = event.getTweakParams();
        else if ( !bOptViz )
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

        int distStrength = 1;

        switch ( event.getTweakType().toLowerCase() )  {

          case TweaksSQL.ENSURE: {

            if(ttime.expired()) 
             ttime = new TupleTime();

            String rTuples = TupleTime.rTuplesMap.get(event.getSpace());
            Feature estimateFtr = executeQueryWithRetry(SQLQueryBuilder.buildEstimateSamplingStrengthQuery(event, bbox, rTuples )).getFeatures().get(0);
            int rCount = estimateFtr.get("rcount");

            if( rTuples == null )
             TupleTime.rTuplesMap.put(event.getSpace(), estimateFtr.get("rtuples") );

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
              if( !bMvtRequested )
              { FeatureCollection collection = executeQueryWithRetrySkipIfGeomIsNull(SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, tweakParams, dataSource));
                if( distStrength > 0 ) collection.setPartial(true); // either ensure mode or explicit tweaks:sampling request where strenght in [1..100]
                return collection;
              }
              else
               return executeBinQueryWithRetry(
                         SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, tweakParams, dataSource) , mercatorTile, bbox, mvtMargin, bMvtFlattend ) );
            }
            else
            { // fall thru tweaks=simplification e.g. mode=viz and vizSampling=off
              tweakParams.put("algorithm", new String("gridbytilelevel"));
            }
          }

          case TweaksSQL.SIMPLIFICATION: {
            if( !bMvtRequested )
            { FeatureCollection collection = executeQueryWithRetrySkipIfGeomIsNull(SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, tweakParams, dataSource));
              return collection;
            }
            else
             return executeBinQueryWithRetry(
               SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, tweakParams, dataSource) , mercatorTile, bbox, mvtMargin, bMvtFlattend ) );
          }

          default: break; // fall back to non-tweaks usage.
        }
      }

      if( bClustering )
      { final Map<String, Object> clusteringParams = event.getClusteringParams();

        switch(event.getClusteringType().toLowerCase())
        {
          case H3SQL.HEXBIN :
           if( !bMvtRequested )
            return executeQueryWithRetry(SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, clusteringParams,dataSource));
           else
            return executeBinQueryWithRetry(
             SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, clusteringParams,dataSource), mercatorTile, bbox, mvtMargin, bMvtFlattend ) );

          case QuadbinSQL.QUAD :
           final int relResolution = ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) :
                                     ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) : 0)),
                     absResolution = clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) : 0;
           final String countMode = (String) clusteringParams.get(QuadbinSQL.QUADBIN_COUNTMODE);
           final boolean noBuffer = (boolean) clusteringParams.getOrDefault(QuadbinSQL.QUADBIN_NOBOFFER,false);

           QuadbinSQL.checkQuadbinInput(countMode, relResolution, event, config.readTableFromEvent(event), streamId, this);

            if( !bMvtRequested )
              return executeQueryWithRetry(SQLQueryBuilder.buildQuadbinClusteringQuery(event, bbox, relResolution, absResolution, countMode, config, noBuffer));
            else
              return executeBinQueryWithRetry(
                      SQLQueryBuilder.buildMvtEncapsuledQuery(config.readTableFromEvent(event), SQLQueryBuilder.buildQuadbinClusteringQuery(event, bbox, relResolution, absResolution, countMode, config, noBuffer), mercatorTile, bbox, mvtMargin, bMvtFlattend ) );

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

      if( !bMvtRequested )
       return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event, isBigQuery, dataSource));
      else
       return executeBinQueryWithRetry( SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event, isBigQuery, dataSource), mercatorTile, bbox, mvtMargin, bMvtFlattend ) );

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
    if(! (event instanceof SearchForFeaturesOrderByEvent) )
     return findFeatures(event, null, false);
    else
     return findFeaturesSort( (SearchForFeaturesOrderByEvent) event );
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


      if (config.getConnectorParams().isIgnoreCreateMse())
        return new SuccessResponse().withStatus("OK");

      validateModifySpaceEvent(event);

      Space spaceDef = event.getSpaceDefinition();

      if(  spaceDef != null && spaceDef.getPartitions() != null 
         && !spaceDef.isEnableHistory()
         && ModifySpaceEvent.Operation.CREATE == event.getOperation()
        )
      { 
        ensureSpace( new PartitionDef( spaceDef.getPartitions() ) ); 
      }  

      if(event.getSpaceDefinition() != null && event.getSpaceDefinition().isEnableHistory()){
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

      if ((ModifySpaceEvent.Operation.CREATE == event.getOperation()
              || ModifySpaceEvent.Operation.UPDATE == event.getOperation())
              && config.getConnectorParams().isPropertySearch()) {

          executeUpdateWithRetry(  SQLQueryBuilder.buildSearchablePropertiesUpsertQuery(
                  event.getSpaceDefinition(),
                  event.getOperation(),
                  config.getDatabaseSettings().getSchema(),
                  config.readTableFromEvent(event))
          );

          dbMaintainer.maintainSpace(traceItem, config.getDatabaseSettings().getSchema(), config.readTableFromEvent(event));
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

  @Override
  protected XyzResponse processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    try {
      logger.info("{} Received " + event.getClass().getSimpleName(), traceItem);
      return executeGetStorageStatistics(event);
    }
    catch (SQLException e) {
      return checkSQLException(e, config.readTableFromEvent(event));
    }
    finally {
      logger.info("{} Finished " + event.getClass().getSimpleName(), traceItem);
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

  protected XyzResponse findFeatures(SearchForFeaturesEvent event, final String handle, final boolean isIterate)
          throws Exception{
    try{
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);

      final SQLQuery searchQuery = SQLQueryBuilder.generateSearchQuery(event,dataSource);
      final boolean hasSearch = searchQuery != null;
      final boolean hasHandle = handle != null;
      final long start = hasHandle ? Long.parseLong(handle) : 0L;

      // For testing purposes.
      if (event.getSpace().contains("illegal_argument")) {
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
        collection.setNextPageToken("" + (start + event.getLimit()));
      }

      return collection;
    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished "+event.getClass().getSimpleName(), traceItem);
    }
  }

  private void setEventValuesFromHandle(SearchForFeaturesOrderByEvent event, String handle) throws JsonMappingException, JsonProcessingException
  {
    ObjectMapper om = new ObjectMapper();
    JsonNode jn = om.readTree(handle);
    String ps = jn.get("p").toString();
    String ts = jn.get("t").toString();
    String ms = jn.get("m").toString();
    PropertiesQuery pq = om.readValue( ps, PropertiesQuery.class );
    TagsQuery tq = om.readValue( ts, TagsQuery.class );
    Integer[] part = om.readValue(ms,Integer[].class);

    event.setPart(part);
    event.setPropertiesQuery(pq);
    event.setTags(tq);
    event.setHandle(handle);
  }

  private String addEventValuesToHandle(SearchForFeaturesOrderByEvent event, String dbhandle)  throws JsonProcessingException
  {
   ObjectMapper om = new ObjectMapper();
   String pQry = String.format( ",\"p\":%s", event.getPropertiesQuery() != null ? om.writeValueAsString(event.getPropertiesQuery()) : "[]" ),
          tQry = String.format( ",\"t\":%s", event.getTags() != null ? om.writeValueAsString(event.getTags()) : "[]" ),
          mQry = String.format( ",\"m\":%s", event.getPart() != null ? om.writeValueAsString(event.getPart()) : "[]" ),
          hndl = String.format("%s%s%s%s}", dbhandle.substring(0, dbhandle.lastIndexOf("}")), pQry, tQry, mQry );
   return hndl;
  }

  private List<String> translateSortSysValues(List<String> sort)
  { if( sort == null ) return null;
    List<String> r = new ArrayList<String>();
    for( String f : sort )      // f. sysval replacements - f.sysval:desc -> sysval:desc
     if( f.toLowerCase().startsWith("f.createdat" ) || f.toLowerCase().startsWith("f.updatedat" ) )
      r.add( f.replaceFirst("^f\\.", "properties.@ns:com:here:xyz.") );
     else
      r.add( f.replaceFirst("^f\\.", "") );

    return r;
  }

  private static final String HPREFIX = "h07~";

  private List<String> getSearchKeys(  PropertiesQuery p )
  { return p.stream()
             .flatMap(List::stream)
             .filter(k -> k.getKey() != null && k.getKey().length() > 0)
             .map(PropertyQuery::getKey)
             .collect(Collectors.toList());
  }

  private List<String> getSortFromSearchKeys( List<String> searchKeys, String space, PSQLXyzConnector connector ) throws Exception
  {
   List<String> indices = Capabilities.IndexList.getIndexList(space, connector);
   if( indices == null ) return null;

   indices.sort((s1, s2) -> s1.length() - s2.length());

   for(String sk : searchKeys )
    switch( sk )
    { case "id" : return null; // none is always sorted by ID;
      case "properties.@ns:com:here:xyz.createdAt" : return Arrays.asList("f.createdAt");
      case "properties.@ns:com:here:xyz.updatedAt" : return Arrays.asList("f.updatedAt");
      default:
       if( !sk.startsWith("properties.") ) sk = "o:f." + sk;
       else sk = sk.replaceFirst("^properties\\.","o:");

       for(String idx : indices)
        if( idx.startsWith(sk) )
        { List<String> r = new ArrayList<String>();
          String[] sortIdx = idx.replaceFirst("^o:","").split(",");
          for( int i = 0; i < sortIdx.length; i++)
           r.add( sortIdx[i].startsWith("f.") ? sortIdx[i] : "properties." + sortIdx[i] );
          return r;
        }
      break;
    }

   return null;
  }

  private String chrE( String s ) { return s.replace('+','-').replace('/','_').replace('=','.'); }
  private String chrD( String s ) { return s.replace('-','+').replace('_','/').replace('.','='); }

  private String createHandle(SearchForFeaturesOrderByEvent event, String jsonData ) throws Exception
  { return HPREFIX + chrE( PSQLConfig.encrypt( addEventValuesToHandle(event, jsonData ) , "findFeaturesSort" )); }

  private XyzResponse requestIterationHandles(SearchForFeaturesOrderByEvent event, int nrHandles ) throws Exception
  {
    event.setPart(null);
    event.setTags(null);

    FeatureCollection cl = executeQueryWithRetry( SQLQueryBuilder.buildGetIterateHandlesQuery(nrHandles));
    List<List<Object>> hdata = cl.getFeatures().get(0).getProperties().get("handles");
    for( List<Object> entry : hdata )
    {
      event.setPropertiesQuery(null);
      if( entry.get(2) != null )
      { PropertyQuery pqry = new PropertyQuery();
        pqry.setKey("id");
        pqry.setOperation(QueryOperation.LESS_THAN);
        pqry.setValues(Arrays.asList( entry.get(2)) );
        PropertiesQuery pqs = new PropertiesQuery();
        PropertyQueryList pql = new PropertyQueryList();
        pql.add( pqry );
        pqs.add( pql );

        event.setPropertiesQuery( pqs );
      }
      entry.set(0, createHandle(event,String.format("{\"h\":\"%s\",\"s\":[]}",entry.get(1).toString())));
    }
    return cl;
  }

  protected XyzResponse findFeaturesSort(SearchForFeaturesOrderByEvent event ) throws Exception
  {
    try{
      logger.info("{} - Received "+event.getClass().getSimpleName(), traceItem);

      boolean hasHandle = (event.getHandle() != null);
      String space = config.readTableFromEvent(event);

      if( !hasHandle )  // decrypt handle and configure event
      {
        if( event.getPart() != null && event.getPart()[0] == -1 )
         return requestIterationHandles( event, event.getPart()[1] );

        if (!Capabilities.canSearchFor(space, event.getPropertiesQuery(), this)) {
          return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
                  .withErrorMessage("Invalid request parameters. Search for the provided properties is not supported for this space.");
        }

        if( event.getPropertiesQuery() != null && (event.getSort() == null || event.getSort().isEmpty()) )
        {
         event.setSort( getSortFromSearchKeys( getSearchKeys( event.getPropertiesQuery() ), space, this ) );
        }
        else if (!Capabilities.canSortBy(space, event.getSort(), this))
        {
          return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
                  .withErrorMessage("Invalid request parameters. Sorting by for the provided properties is not supported for this space.");
        }

        event.setSort( translateSortSysValues( event.getSort() ));
      }
      else if( !event.getHandle().startsWith( HPREFIX ) )
       return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
               .withErrorMessage("Invalid request parameter. handle is corrupted");
      else
       try { setEventValuesFromHandle(event, PSQLConfig.decrypt( chrD( event.getHandle().substring(HPREFIX.length()) ) ,"findFeaturesSort" ) ); }
       catch ( GeneralSecurityException|IllegalArgumentException e)
       { return new ErrorResponse().withStreamId(streamId).withError(XyzError.ILLEGAL_ARGUMENT)
                 .withErrorMessage("Invalid request parameter. handle is corrupted");
       }

      SQLQuery query = SQLQueryBuilder.buildFeaturesSortQuery(event, dataSource) ;

      FeatureCollection collection = executeQueryWithRetry(query);

      if( collection.getHandle() != null ) // extend handle and encrypt
      { final String handle = createHandle( event, collection.getHandle() );
        collection.setHandle( handle );
        collection.setNextPageToken(handle);
      }

      return collection;
    } catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    } finally {
      logger.info("{} - Finished "+event.getClass().getSimpleName(), traceItem);
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
