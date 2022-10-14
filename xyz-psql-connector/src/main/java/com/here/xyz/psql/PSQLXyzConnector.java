/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT;
import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.MVT_FLATTENED;
import static com.here.xyz.responses.XyzError.EXCEPTION;

import com.amazonaws.services.lambda.runtime.Context;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.DeleteFeaturesByTagEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetFeaturesByTileEvent.ResponseType;
import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.HQuad;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.factory.H3SQL;
import com.here.xyz.psql.factory.QuadbinSQL;
import com.here.xyz.psql.factory.TweaksSQL;
import com.here.xyz.psql.query.GetFeaturesByBBox;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.psql.query.GetFeaturesById;
import com.here.xyz.psql.query.GetStorageStatistics;
import com.here.xyz.psql.query.IterateFeatures;
import com.here.xyz.psql.query.LoadFeatures;
import com.here.xyz.psql.query.SearchForFeatures;
import com.here.xyz.psql.tools.DhString;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PSQLXyzConnector extends DatabaseHandler {

  /**
   * Real live counts via count(*)
   */
  public static final String COUNTMODE_REAL = "real";
  /**
   * Estimated counts, determined with _postgis_selectivity() or EXPLAIN Plan analyze
   */
  public static final String COUNTMODE_ESTIMATED = "estimated";
  /**
   * Combination of real and estimated.
   */
  public static final String COUNTMODE_MIXED = "mixed";
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

  static private class TupleTime
  { static private Map<String,String> rTuplesMap = new ConcurrentHashMap<String,String>();
    long outTs;
    TupleTime() { outTs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15); rTuplesMap.clear(); }
    boolean expired() { return  System.currentTimeMillis() > outTs; }
  }

  static private TupleTime ttime = new TupleTime();

  static private boolean isVeryLargeSpace( String rTuples )
  { long tresholdVeryLargeSpace = 350000000L; // 350M Objects

    if( rTuples == null ) return false;

    String[] a = rTuples.split("~");
    if( a == null || a[0] == null ) return false;

    try{ return tresholdVeryLargeSpace <= Long.parseLong(a[0]); } catch( NumberFormatException e ){}

    return false;
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

      ResponseType responseType = SQLQueryBuilder.getResponseType(event);
      int mvtMargin = 0;
      boolean bMvtRequested = responseType == MVT || responseType == MVT_FLATTENED,
              bMvtFlattend  = responseType == MVT_FLATTENED;

      WebMercatorTile mercatorTile = null;
      HQuad hereTile = null;
      GetFeaturesByTileEvent tileEv = ( event instanceof GetFeaturesByTileEvent ? (GetFeaturesByTileEvent) event : null );

      if( tileEv != null && tileEv.getHereTileFlag() ){
        if(bClustering)
          throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "clustering=[hexbin,quadbin] is not supported for 'here' tile type. Use Web Mercator projection (quadkey, web, tms).");
      }

      if( bMvtRequested && tileEv == null )
       throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "mvt format needs tile request");

      if( bMvtRequested )
      {
        if( event.getConnectorParams() == null || event.getConnectorParams().get("mvtSupport") != Boolean.TRUE )
         throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "mvt format is not supported");

        if(!tileEv.getHereTileFlag())
         mercatorTile = WebMercatorTile.forWeb(tileEv.getLevel(), tileEv.getX(), tileEv.getY());
        else
         hereTile = new HQuad(tileEv.getX(), tileEv.getY(),tileEv.getLevel());

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
        boolean bSortByHashedValue = false;

        switch ( event.getTweakType().toLowerCase() )  {

          case TweaksSQL.ENSURE: {

            if(ttime.expired())
             ttime = new TupleTime();

            String rTuples = TupleTime.rTuplesMap.get(event.getSpace());
            Feature estimateFtr = executeQueryWithRetry(SQLQueryBuilder.buildEstimateSamplingStrengthQuery(event, bbox, rTuples )).getFeatures().get(0);
            int rCount = estimateFtr.get("rcount");

            if( rTuples == null )
            { rTuples = estimateFtr.get("rtuples");
              TupleTime.rTuplesMap.put(event.getSpace(), rTuples );
            }

            bSortByHashedValue = isVeryLargeSpace( rTuples );

            boolean bDefaultSelectionHandling = (tweakParams.get(TweaksSQL.ENSURE_DEFAULT_SELECTION) == Boolean.TRUE );

            if( event.getSelection() == null && !bDefaultSelectionHandling && !bSelectionStar )
             event.setSelection(Arrays.asList("id","type"));

            distStrength = TweaksSQL.calculateDistributionStrength( rCount, Math.max(Math.min((int) tweakParams.getOrDefault(TweaksSQL.ENSURE_SAMPLINGTHRESHOLD,10),100),10) * 1000 );

            HashMap<String, Object> hmap = new HashMap<>();
            hmap.put("algorithm", new String("distribution"));
            hmap.put("strength", new Integer( distStrength ));
            tweakParams = hmap;
            // fall thru tweaks=sampling
          }

          case TweaksSQL.SAMPLING: {
            if( bTweaks || !bVizSamplingOff )
            {
              if( !bMvtRequested )
              { FeatureCollection collection = executeQueryWithRetrySkipIfGeomIsNull(SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, tweakParams, bSortByHashedValue));
                if( distStrength > 0 ) collection.setPartial(true); // either ensure mode or explicit tweaks:sampling request where strenght in [1..100]
                return collection;
              }
              else
               return executeBinQueryWithRetry(
                         SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildSamplingTweaksQuery(event, bbox, tweakParams, bSortByHashedValue) , mercatorTile, hereTile, bbox, mvtMargin, bMvtFlattend ) );
            }
            else
            { // fall thru tweaks=simplification e.g. mode=viz and vizSampling=off
              tweakParams.put("algorithm", "gridbytilelevel");
            }
          }

          case TweaksSQL.SIMPLIFICATION: {
            if( !bMvtRequested )
            { FeatureCollection collection = executeQueryWithRetrySkipIfGeomIsNull(SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, tweakParams));
              return collection;
            }
            else
             return executeBinQueryWithRetry(
               SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildSimplificationTweaksQuery(event, bbox, tweakParams) , mercatorTile, hereTile, bbox, mvtMargin, bMvtFlattend ) );
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
             SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildHexbinClusteringQuery(event, bbox, clusteringParams,dataSource), mercatorTile, hereTile, bbox, mvtMargin, bMvtFlattend ) );

          case QuadbinSQL.QUAD :
           final int relResolution = ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION) :
                                     ( clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_RELATIVE) : 0)),
                     absResolution = clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) != null ? (int) clusteringParams.get(QuadbinSQL.QUADBIN_RESOLUTION_ABSOLUTE) : 0;
           final String countMode = (String) clusteringParams.get(QuadbinSQL.QUADBIN_COUNTMODE);
           final boolean noBuffer = (boolean) clusteringParams.getOrDefault(QuadbinSQL.QUADBIN_NOBOFFER,false);

           checkQuadbinInput(countMode, relResolution, event, streamId);

            if( !bMvtRequested )
              return executeQueryWithRetry(SQLQueryBuilder.buildQuadbinClusteringQuery(event, bbox, relResolution, absResolution, countMode, config, noBuffer));
            else
              return executeBinQueryWithRetry(
                      SQLQueryBuilder.buildMvtEncapsuledQuery(config.readTableFromEvent(event), SQLQueryBuilder.buildQuadbinClusteringQuery(event, bbox, relResolution, absResolution, countMode, config, noBuffer), mercatorTile, hereTile, bbox, mvtMargin, bMvtFlattend ) );

          default: break; // fall back to non-tweaks usage.
       }
      }

      final boolean isBigQuery = (bbox.widthInDegree(false) >= (360d / 4d) || (bbox.heightInDegree() >= (180d / 4d)));

      if (isBigQuery)
        //Check if Properties are indexed
        checkCanSearchFor(event);

      if (event.getParams() != null && event.getParams().containsKey("extends") && event.getContext() == DEFAULT)
        return new GetFeaturesByBBox<>(event, this).run();

      if( !bMvtRequested )
       return executeQueryWithRetry(SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event));
      else
       return executeBinQueryWithRetry( SQLQueryBuilder.buildMvtEncapsuledQuery(event.getSpace(), SQLQueryBuilder.buildGetFeaturesByBBoxQuery(event), mercatorTile, hereTile, bbox, mvtMargin, bMvtFlattend ) );

    }catch (SQLException e){
      return checkSQLException(e, config.readTableFromEvent(event));
    }finally {
      logger.info("{} Finished "+event.getClass().getSimpleName(), traceItem);
    }
  }

  /**
   * Check if request parameters are valid. In case of invalidity throw an Exception
   */
  private void checkQuadbinInput(String countMode, int relResolution, GetFeaturesByBBoxEvent event, String streamId) throws ErrorResponseException
  {
    if(countMode != null && (!countMode.equalsIgnoreCase(COUNTMODE_REAL) && !countMode.equalsIgnoreCase(COUNTMODE_ESTIMATED) && !countMode.equalsIgnoreCase(
        COUNTMODE_MIXED)) )
      throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
          "Invalid request parameters. Unknown clustering.countmode="+countMode+". Available are: ["+ COUNTMODE_REAL
              +","+ COUNTMODE_ESTIMATED +","+ COUNTMODE_MIXED +"]!");

    if(relResolution > 5)
      throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
          "Invalid request parameters. clustering.relativeResolution="+relResolution+" to high. 5 is maximum!");

    if(event.getPropertiesQuery() != null && event.getPropertiesQuery().get(0).size() != 1)
      throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
          "Invalid request parameters. Only one Property is allowed");

    checkCanSearchFor(event);
  }

  @Override
  protected XyzResponse processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    return processGetFeaturesByBBoxEvent(event);
  }

  @Override
  protected XyzResponse processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    try {
      logger.info("{} Received "+event.getClass().getSimpleName(), traceItem);
      checkCanSearchFor(event);

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
      checkCanSearchFor(event);

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

  private void checkCanSearchFor(SearchForFeaturesEvent event) throws ErrorResponseException {
    if (!Capabilities.canSearchFor(config.readTableFromEvent(event), event.getPropertiesQuery(), this))
      throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT,
          "Invalid request parameters. Search for the provided properties is not supported for this space.");
  }

  @Override
  @Deprecated
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
