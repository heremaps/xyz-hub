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

package com.here.xyz.httpconnector.rest;

import com.here.xyz.httpconnector.config.JDBCMaintainer;
import com.here.xyz.httpconnector.rest.HApiParam.HQuery;
import com.here.xyz.httpconnector.task.MaintenanceHandler;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.json.DecodeException;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.pgclient.PgException;

import javax.naming.NoPermissionException;

import java.io.IOException;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class HttpMaintenanceApi extends Api {

  public HttpMaintenanceApi(RouterBuilder rb) {
    rb.operation("getStatus").handler(this::getConnectorStatus);
    rb.operation("postInitialization").handler(this::postDatabaseInitialization);
    rb.operation("postMaintainIndices").handler(this::postMaintainIndices);
    rb.operation("postPurgeVersions").handler(this::postPurgeVersions); //TODO: Move responsibility back to connector

    rb.operation("getMaintenanceStatusSpace").handler(this::getMaintenanceStatusSpace);
    rb.operation("postMaintainSpace").handler(this::postMaintainSpace);
  }

  private void getConnectorStatus(final RoutingContext context) {
    String connectorId =  context.pathParam(HApiParam.Path.CONNECTOR_ID);
    MaintenanceHandler.getConnectorStatus(connectorId)
            .onFailure(e -> this.sendErrorResponse(context, e))
            .onSuccess(j -> this.sendResponse(context, OK, j));
  }

  private void postDatabaseInitialization(final RoutingContext context) {
    String connectorId =  context.pathParam(HApiParam.Path.CONNECTOR_ID);
    final boolean force = HQuery.getBoolean(context, "force", false);

    MaintenanceHandler.initializeOrUpdateDatabase(connectorId, force)
            .onFailure(e -> this.sendErrorResponse(context, e))
            .onSuccess(j -> this.sendResponse(context, OK, j));
  }

  private void postMaintainIndices(final RoutingContext context) {
    String connectorId =  context.pathParam(HApiParam.Path.CONNECTOR_ID);
    //TODO: remove AutoIndexing
    final boolean autoIndexing = HQuery.getBoolean(context, "autoIndexing", false);

    MaintenanceHandler.maintainIndices(connectorId, autoIndexing)
            .onFailure(e -> this.sendErrorResponse(context, e))
            .onSuccess(j -> this.sendResponse(context, OK, j));
  }

  private void postPurgeVersions(final RoutingContext context) {
    String spaceId = context.pathParam(HApiParam.Path.SPACE_ID);
    final Long minTagVersion = HQuery.getLong(context, "minTagVersion", null);

    MaintenanceHandler.purgeOldVersions(spaceId, minTagVersion)
            .onFailure(e -> checkException(context, null, spaceId, e))
            .onSuccess(j -> this.sendResponse(context, OK, j));
  }

  private void postMaintainSpace(final RoutingContext context) {
    String spaceId =  context.pathParam(HApiParam.Path.SPACE_ID);
    final boolean force = HQuery.getBoolean(context, "force", false);

    MaintenanceHandler.maintainSpace(spaceId, force)
            .onFailure(e -> checkException(context, null, spaceId, e))
            .onSuccess(j -> this.sendResponse(context, OK, j));
  }

  private void getMaintenanceStatusSpace(final RoutingContext context) {
    String spaceId = context.pathParam(HApiParam.Path.SPACE_ID);

    MaintenanceHandler.getMaintenanceStatusOfSpace(spaceId)
            .onFailure(e -> checkException(context, null, spaceId, e))
            .onSuccess(j -> this.sendResponse(context, OK, j));
  }

  private void checkException(final RoutingContext context, String connector, String spaceId, Throwable e){
    HttpException httpException;
    String source = connector != null ? "Connector["+connector+"]: " : "Space["+spaceId+"]: ";
    if(e instanceof HttpException) {
      httpException = (HttpException)e;
    }else if(e instanceof PgException) {
      if(((PgException) e).getSqlState() != null){
        logger.warn("{} {}", source, shortenedMessage(e));
        switch (((PgException) e).getSqlState()){
          case "42501" :
            httpException = new HttpException(UNAUTHORIZED, "Permission denied!");
            break;
          case "57014" :
            httpException =  new HttpException(GATEWAY_TIMEOUT, "Query got aborted due to timeout!");
            break;
          default:
            httpException = new HttpException(BAD_GATEWAY, "SQL Query error "+((PgException) e).getSqlState()+"!");
        }
      }
      else {
        logger.warn("{} {}", source, shortenedMessage(e));
        httpException =  new HttpException(BAD_GATEWAY, "SQL Query error!");
      }
    }else if (e instanceof NoPermissionException) {
      logger.warn("{} {}", source, e.getMessage());
      httpException =  new HttpException(UNAUTHORIZED, "Database user dose not have the required permissions!");
    }else if (e instanceof IOException) {
      logger.warn("{} {}", source, e.getMessage());
      httpException = new HttpException(BAD_GATEWAY, "Cant execute database script!");
    }else if (e instanceof DecodeException) {
      logger.warn("{} {}", source, e.getMessage());
      httpException = new HttpException(BAD_REQUEST, "Cant decrypt ECPS - check passphrase!");
    }else{
      if( e.getMessage() != null && e.getMessage().equals(JDBCMaintainer.SPACE_NOT_FOUND_OR_INVALID)) {
        httpException = new HttpException(NOT_FOUND, null);
      }else if( e.getMessage() != null && e.getMessage().equals(JDBCMaintainer.SPACE_MAINTENANCE_ENTRY_NOT_VALID)) {
        httpException = new HttpException(BAD_GATEWAY, "Cant serialize MaintenanceStatus!");
      }else if( e.getMessage() != null && e.getMessage().equals(JDBCMaintainer.CONNECTOR_MAINTENANCE_ENTRY_NOT_VALID)) {
        httpException = new HttpException(BAD_GATEWAY, "Cant serialize ConnectorStatus!");
      }else {
        logger.error("{} {}", source, e.getMessage());
        httpException = new HttpException(BAD_GATEWAY, "Unexpected Exception!");
      }
    }

    this.sendErrorResponse(context, httpException);
  }

  private String shortenedMessage(Throwable e){
    if(e == null  || e.getMessage() == null)
      return null;
    return  e.getMessage().length() > 300 ? e.getMessage().substring(0, 300) + "..." : e.getMessage();
  }

}