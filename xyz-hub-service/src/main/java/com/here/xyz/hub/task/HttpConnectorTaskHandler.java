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

package com.here.xyz.hub.task;

import com.here.xyz.hub.Core;
import com.here.xyz.hub.HttpConnector;
import com.here.xyz.hub.PsqlHttpVerticle;
import com.here.xyz.hub.rest.HttpException;

import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.maintenance.ConnectorStatus;
import com.here.xyz.responses.maintenance.SpaceStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.DecodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.NoPermissionException;
import java.io.IOException;
import java.sql.SQLException;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class HttpConnectorTaskHandler {
  private static final Logger logger = LogManager.getLogger();

  public static void getConnectorStatus(String connectorId, String ecps, String passphrase, Handler<AsyncResult<XyzResponse>> handler) {
    ConnectorStatus connectorStatus;

    try {
      connectorStatus = HttpConnector.maintenanceClient.getConnectorStatus(connectorId, ecps, passphrase);
      if(connectorStatus == null){
        handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "Cant get status for connector: "+connectorId)));
        return;
      }
      handler.handle(Future.succeededFuture(connectorStatus));
    }catch (Exception e) {
      checkException(e, handler);
    }
  }

  public static void initializeDatabase(String connectorId, String ecps, String passphrase, boolean force, Handler<AsyncResult<XyzResponse>> handler) {
    try {
      ConnectorStatus dbStatus = force ? null : HttpConnector.maintenanceClient.getConnectorStatus(connectorId, ecps, passphrase);
      if(dbStatus != null && dbStatus.isInitialized()) {
        /** Nothing more to do */
        logger.info("Database is already initialized for connector: {}",connectorId);
      } else {
        logger.info("Start database initialization for connector: {} ",connectorId);
        HttpConnector.maintenanceClient.initializeEmptyDatabase(connectorId, ecps, passphrase, force);
      }
      handler.handle(Future.succeededFuture(new SuccessResponse().withStatus("Ok")));
    }catch (Exception e) {
      checkException(e, handler);
    }
  }

  public static void maintainIndices(String connectorId, String ecps, String passphrase, boolean autoIndexing, Handler<AsyncResult<XyzResponse>> handler) {
    try {
      ConnectorStatus connectorStatus = HttpConnector.maintenanceClient.getConnectorStatus(connectorId, ecps, passphrase);
      if(connectorStatus != null && connectorStatus.isInitialized()) {
        if(connectorStatus.getMaintenanceStatus() != null && connectorStatus.getMaintenanceStatus().get(ConnectorStatus.AUTO_INDEXING) !=  null) {
          ConnectorStatus.MaintenanceStatus autoIndexingStatus = connectorStatus.getMaintenanceStatus().get(ConnectorStatus.AUTO_INDEXING) ;

          if(autoIndexingStatus.getMaintenanceRunning().size() > 0 ){
            Long timeSinceLastRunInHr = (Core.currentTimeMillis() - autoIndexingStatus.getMaintainedAt()) / 1000 / 60 / 60;
            if(timeSinceLastRunInHr > PsqlHttpVerticle.MISSING_MAINTENANCE_WARNING_IN_HR)
              logger.warn("Last MaintenanceRun is older than {}h - connector: {}", timeSinceLastRunInHr, connectorId);
          }

          if(autoIndexingStatus.getMaintenanceRunning().size() >=  PsqlHttpVerticle.MAX_CONCURRENT_MAINTENANCE_TASKS) {
            handler.handle(Future.failedFuture(new HttpException(CONFLICT, "Maximal concurrent Indexing tasks are running!")));
            return;
          }
        }
        logger.info("Start maintain indices for connector: {}", connectorId);
        HttpConnector.maintenanceClient.maintainIndices(connectorId, ecps, passphrase, autoIndexing);
        handler.handle(Future.succeededFuture(new SuccessResponse().withStatus("Ok")));
      } else {
        logger.warn("Database not initialized for connector: {}",connectorId);
        handler.handle(Future.failedFuture(new HttpException(METHOD_NOT_ALLOWED, "Database not initialized!")));
        return;
      }
    }catch (Exception e) {
      checkException(e, handler);
    }
  }

  public static void maintainSpace(String connectorId, String ecps, String passphrase, String spaceId,
                                  boolean force, Handler<AsyncResult<XyzResponse>> handler) {
    try {

      if(!force) {
        SpaceStatus maintenanceStatusOfSpace = HttpConnector.maintenanceClient.getMaintenanceStatusOfSpace(connectorId, ecps, passphrase, spaceId);
        if (maintenanceStatusOfSpace != null && maintenanceStatusOfSpace.isIdxCreationFinished() != null && !maintenanceStatusOfSpace.isIdxCreationFinished()) {
          logger.warn("Index creation is currently running on {}/{}", spaceId, connectorId);
          handler.handle(Future.failedFuture(new HttpException(CONFLICT, "Index creation is currently running!")));
          return;
        }
      }

      HttpConnector.maintenanceClient.maintainSpace(connectorId, ecps, passphrase, spaceId);
      /** If the space does not exists we return also OK */
      handler.handle(Future.succeededFuture(new SuccessResponse().withStatus("Ok")));
    }catch (SQLException e){
      if(e.getSQLState() != null && e.getSQLState().equals("42P01")){
        logger.warn("Connector is not initialized properly: {}/{}", spaceId, connectorId);
        handler.handle(Future.failedFuture(new HttpException(CONFLICT, "Database is not initialized properly!")));
        return;
      }
      checkException(e, handler);
    }catch (Exception e) {
      checkException(e, handler);
    }
  }

  public static void maintainHistory(String connectorId, String ecps, String passphrase, String spaceId, int currentVersion, int maxVersionCount,
                                   Handler<AsyncResult<XyzResponse>> handler) {

    try {
      HttpConnector.maintenanceClient.maintainHistory(connectorId, ecps, passphrase, spaceId, currentVersion, maxVersionCount);
      handler.handle(Future.succeededFuture(new SuccessResponse().withStatus("Ok")));
    }catch (SQLException e){
      if(e.getSQLState() != null && e.getSQLState().equals("42P01")){
        logger.warn("History Table does not exits {}/{}", spaceId, connectorId);
        handler.handle(Future.failedFuture(new HttpException(CONFLICT, "History Table does not exists!")));
        return;
      }else
        checkException(e, handler);
    }catch (Exception e) {
      checkException(e, handler);
    }
  }

  public static void getMaintenanceStatusOfSpace(String connectorId, String ecps, String passphrase, String spaceId,
                                                 Handler<AsyncResult<XyzResponse>> handler) {
    try {
      SpaceStatus maintenanceStatusOfSpace = HttpConnector.maintenanceClient.getMaintenanceStatusOfSpace(connectorId, ecps, passphrase, spaceId);
      if(maintenanceStatusOfSpace == null) {
        handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "Can not find entry in idx-table: "+spaceId)));
        return;
      }
      handler.handle(Future.succeededFuture(maintenanceStatusOfSpace));
    }catch (Exception e) {
      checkException(e, handler);
    }
  }

  private static void checkException(Exception e, Handler<AsyncResult<XyzResponse>> handler){

    logger.error(e);

    if(e instanceof SQLException) {
      if(((SQLException) e).getSQLState() != null && ((SQLException) e).getSQLState().equalsIgnoreCase("57014")){
        handler.handle(Future.failedFuture(new HttpException(GATEWAY_TIMEOUT, "Query got aborted due to timeout!")));
        return;
      }
      else if(((SQLException) e).getSQLState() == null && e.getMessage().equalsIgnoreCase("An attempt by a client to checkout a connection has timed out.")){
        handler.handle(Future.failedFuture(new HttpException(GATEWAY_TIMEOUT, "Could not get a connection to the database!")));
        return;
      }
      handler.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "SQL Query error!")));
    }
    else if (e instanceof NoPermissionException)
      handler.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Database user dose not have the required permissions!")));
    else if (e instanceof IOException)
      handler.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Cant execute database script!")));
    else if (e instanceof DecodeException)
      handler.handle(Future.failedFuture(new HttpException(BAD_REQUEST, "Cant decrypt ECPS - check passphrase!")));
    else
      handler.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Unexpected Exception!")));
  }
}
