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

import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;
import static com.here.xyz.responses.XyzError.PAYLOAD_TO_LARGE;
import static com.here.xyz.responses.XyzError.TIMEOUT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.psql.query.GetFastStatistics;
import com.here.xyz.util.runtime.FunctionRuntime;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.query.DeleteChangesets;
import com.here.xyz.psql.query.GetChangesetStatistics;
import com.here.xyz.psql.query.GetFeaturesByBBox;
import com.here.xyz.psql.query.GetFeaturesByBBoxClustered;
import com.here.xyz.psql.query.GetFeaturesByBBoxTweaked;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.psql.query.GetFeaturesById;
import com.here.xyz.psql.query.GetStatistics;
import com.here.xyz.psql.query.GetStorageStatistics;
import com.here.xyz.psql.query.IterateChangesets;
import com.here.xyz.psql.query.IterateFeatures;
import com.here.xyz.psql.query.LoadFeatures;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.psql.query.ModifySubscription;
import com.here.xyz.psql.query.SearchForFeatures;
import com.here.xyz.psql.query.WriteFeatures;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.db.SQLQuery;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PSQLXyzConnector extends DatabaseHandler {
  private static final Logger logger = LogManager.getLogger();
  private static final Pattern ERRVALUE_22P02 = Pattern.compile("invalid input syntax for type numeric:\\s+\"([^\"]*)\"\\s+Query:"),
      ERRVALUE_22P05 = Pattern.compile("ERROR:\\s+(.*)\\s+Detail:\\s+(.*)\\s+Where:");

  @Override
  protected HealthStatus processHealthCheckEvent(HealthCheckEvent event) throws Exception {
    if (event.getWarmupCount() == 0 && FunctionRuntime.getInstance().isRunningLocally()) {
      //FIXME: Do not trigger maintenance on warmup calls, but on simple health-check (non-warmup) calls instead!
      //Run DB-Maintenance - warmUp request is used
      if (event.getMinResponseTime() != 0) {
        logger.info("{} dbMaintainer start", traceItem);
        dbMaintainer.run(traceItem);
        logger.info("{} dbMaintainer finished", traceItem);
        return new HealthStatus().withStatus("OK");
      }
    }

    SQLQuery query = new SQLQuery("SELECT 1");
    query.run(dataSourceProvider);

    //Run health-check query on the replica, if such is set.
    if (dataSourceProvider.hasReader())
      query.run(dataSourceProvider, true);

    return ((HealthStatus) super.processHealthCheckEvent(event)).withStatus("OK");
  }

  @Override
  protected StatisticsResponse processGetStatistics(GetStatisticsEvent event) throws Exception {
    if(event.isFastMode())
      return run(new GetFastStatistics(event));
    return run(new GetStatistics(event));
  }

  @Override
  protected FeatureCollection processGetFeaturesByIdEvent(GetFeaturesByIdEvent event) throws Exception {
    return run(new GetFeaturesById(event));
  }

  @Override
  protected FeatureCollection processGetFeaturesByGeometryEvent(GetFeaturesByGeometryEvent event) throws Exception {
    return run(new GetFeaturesByGeometry(event));
  }

  @Override
  protected FeatureCollection processGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    checkForInvalidHereTileClustering(event);
    return run(getBBoxBasedQueryRunner(event));
  }

  @Override
  protected BinaryResponse processBinaryGetFeaturesByTileEvent(GetFeaturesByTileEvent event) throws Exception {
    if (!mvtSupported(event))
      //NOTE: For this connector implementation, we never want to fall back to the MVT transformation within the service
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "MVT format is not supported");

    checkForInvalidHereTileClustering(event);
    return run(getBBoxBasedQueryRunner(event));
  }

  private static void checkForInvalidHereTileClustering(GetFeaturesByTileEvent event) throws ErrorResponseException {
    if (event.getHereTileFlag() && event.getClusteringType() != null)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT,
          "clustering=[hexbin,quadbin] is not supported for 'here' tile type. Use Web Mercator projection (quadkey, web, tms).");
  }

  @Override
  protected FeatureCollection processGetFeaturesByBBoxEvent(GetFeaturesByBBoxEvent event) throws Exception {
    return run(getBBoxBasedQueryRunner(event));
  }

  private <R extends XyzResponse> GetFeaturesByBBox<GetFeaturesByBBoxEvent, R> getBBoxBasedQueryRunner(GetFeaturesByBBoxEvent event)
      throws Exception {
    if (event.getClusteringType() != null)
      return new GetFeaturesByBBoxClustered<>(event);
    if (event.getTweakType() != null || "viz".equals(event.getOptimizationMode()))
      return new GetFeaturesByBBoxTweaked<>(event);
    return new GetFeaturesByBBox<>(event);
  }

  @Override
  protected FeatureCollection processIterateFeaturesEvent(IterateFeaturesEvent event) throws Exception {
    return run(new IterateFeatures(event));
  }

  @Override
  protected FeatureCollection processSearchForFeaturesEvent(SearchForFeaturesEvent event) throws Exception {
    return run(new SearchForFeatures<>(event));
  }

  @Override
  protected FeatureCollection processLoadFeaturesEvent(LoadFeaturesEvent event) throws Exception {
    return run(new LoadFeatures(event));
  }

  @Override
  protected FeatureCollection processModifyFeaturesEvent(ModifyFeaturesEvent event) throws Exception {
    return executeModifyFeatures(event);
  }

  @Override
  protected FeatureCollection processWriteFeaturesEvent(WriteFeaturesEvent event) throws Exception {
    return run(new WriteFeatures(event));
  }

  @Override
  protected SuccessResponse processModifySpaceEvent(ModifySpaceEvent event) throws Exception {
    return write(new ModifySpace(event).withDbMaintainer(dbMaintainer));
  }

  @Override
  protected SuccessResponse processModifySubscriptionEvent(ModifySubscriptionEvent event) throws Exception {
    return write(new ModifySubscription(event));
  }

  @Override
  protected StorageStatistics processGetStorageStatisticsEvent(GetStorageStatisticsEvent event) throws Exception {
    return run(new GetStorageStatistics(event));
  }

  @Override
  protected SuccessResponse processDeleteChangesetsEvent(DeleteChangesetsEvent event) throws Exception {
    return write(new DeleteChangesets(event));
  }

  @Override
  protected ChangesetCollection processIterateChangesetsEvent(IterateChangesetsEvent event) throws Exception {
    return run(new IterateChangesets(event));
  }

  @Override
  protected ChangesetsStatisticsResponse processGetChangesetsStatisticsEvent(GetChangesetStatisticsEvent event) throws Exception {
    return run(new GetChangesetStatistics(event));
  }

  @Override
  protected void handleProcessingException(Exception exception, Event event) throws Exception {
    if (!(exception instanceof SQLException sqlException))
      throw exception;

    checkSQLException(sqlException, XyzEventBasedQueryRunner.readTableFromEvent(event));
  }

  //TODO: Move error handling into according QueryRunners
  private void checkSQLException(SQLException e, String table) throws ErrorResponseException {
    logger.warn("{} SQL Error ({}) on {} :", traceItem, e.getSQLState(), table, e);

    String sqlState = e.getSQLState() != null ? e.getSQLState().toUpperCase() : "SNULL";

    switch (sqlState) {
      case "XX000": //XX000 - internal error
        if (e.getMessage() == null)
          break;
        if (e.getMessage().indexOf("interruptedException") != -1)
          break;
        if (e.getMessage().indexOf("ERROR: stats for") != -1)
          throw new ErrorResponseException(ILLEGAL_ARGUMENT, "statistical data for this space is missing (analyze)");
        if (e.getMessage().indexOf("TopologyException") != -1)
          throw new ErrorResponseException(ILLEGAL_ARGUMENT, "geometry with irregular topology (self-intersection, clipping)");
        if (e.getMessage().indexOf("ERROR: transform: couldn't project point") != -1)
          throw new ErrorResponseException(ILLEGAL_ARGUMENT, "projection error");
        if (e.getMessage().indexOf("ERROR: encode_geometry: 'GeometryCollection'") != -1)
          throw new ErrorResponseException(ILLEGAL_ARGUMENT, "dataset contains invalid geometries");
        if (e.getMessage().indexOf("ERROR: can not mix dimensionality in a geometry") != -1)
          throw new ErrorResponseException(ILLEGAL_ARGUMENT, "can not mix dimensionality in a geometry");
        //fall thru - timeout assuming timeout

      case "57014": //57014 - query_canceled
      case "57P01": //57P01 - admin_shutdown
        throw new ErrorResponseException(TIMEOUT, "Database query timed out or got canceled.");

      case "54000":
        throw new ErrorResponseException(TIMEOUT, "No time for retry left for database query.");

      case "22P02": {
        //Specific handling in case to H3 clustering.property
        if (e.getMessage() == null)
          break;
        else if (e.getMessage().contains("'H3'::text")) {
          Matcher m = ERRVALUE_22P02.matcher(e.getMessage());
          throw new ErrorResponseException(ILLEGAL_ARGUMENT, "clustering.property: string(" + (m.find() ? m.group(1) : "") + ") can not be converted to numeric");
        }
        else
          throw new ErrorResponseException(ILLEGAL_ARGUMENT, e.getMessage());
      }

      case "22P05": {
        if (e.getMessage() == null)
          break;
        String eMsg = "untranslatable character in payload";
        Matcher m = ERRVALUE_22P05.matcher(e.getMessage());

        if (m.find())
          eMsg = eMsg + ": " + m.group(1) + " - " + m.group(2);

        throw new ErrorResponseException(ILLEGAL_ARGUMENT, eMsg);
      }

      case "42P01":
        int messagePrefixLengthToReport = 75;
        throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Table not found in database: " + table + 
                                                           (e.getMessage() == null || e.getMessage().length() <= messagePrefixLengthToReport
                                                            ? "" 
                                                            : " - " + e.getMessage().substring(0,messagePrefixLengthToReport) ));

      case
          "40P01": // Database -> deadlock detected e.g. "Process 9452 waits for ShareLock on transaction 2383228826; blocked by process 9342."
        throw new ErrorResponseException(EXCEPTION, e.getMessage());

      case "SNULL":
        if (e.getMessage() == null)
          break;
        //Handle some dedicated messages
        if (e.getMessage().indexOf("An attempt by a client to checkout a connection has timed out.") > -1)
          throw new ErrorResponseException(TIMEOUT, "Cannot get a connection to the database.");

        if (e.getMessage().indexOf("Maxchar limit") > -1)
          throw new ErrorResponseException(PAYLOAD_TO_LARGE, "Database result - Maxchar limit exceed");

        break; //others

      default:
        break;
    }

    throw new ErrorResponseException(EXCEPTION, e.getMessage());
  }
}
