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

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ConnectorApi;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.diff.Difference.DiffMap;
import com.here.xyz.hub.util.diff.Patcher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class ConnectorHandler {

  private static final int ID_MIN_LENGTH = 4;
  private static final int ID_MAX_LENGTH = 64;
  private static final Logger logger = LogManager.getLogger();

  public static void getConnector(RoutingContext context, String connectorId, Handler<AsyncResult<Connector>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.get(marker, connectorId, ar -> {
      if (ar.failed()) {
        logger.warn(marker, "The requested resource does not exist.'", ar.cause());
        handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "The requested resource does not exist.", ar.cause())));
      }
      else {
        handler.handle(Future.succeededFuture(ar.result()));
      }
    });
  }

  public static void getConnectors(RoutingContext context, List<String> connectorIds, Handler<AsyncResult<List<Connector>>> handler) {
    Marker marker = Api.Context.getMarker(context);

    List<CompletableFuture<Connector>> completableFutureList = new ArrayList<>();
    connectorIds.forEach(connectorId -> {
      CompletableFuture<Connector> f = new CompletableFuture<>();
      completableFutureList.add(f);
      Service.connectorConfigClient.get(marker, connectorId, ar -> {
        if (ar.failed()) {
          logger.warn(marker, "The requested resource does not exist.'", ar.cause());
          f.completeExceptionally(new HttpException(NOT_FOUND, "The requested resource '" + connectorId + "' does not exist.", ar.cause()));
        } else {
          f.complete(ar.result());
        }
      });
    });

    CompletableFuture.allOf(completableFutureList.toArray(new CompletableFuture[0]))
        .exceptionally(getConnectorEx -> {
          HttpException ex = (HttpException) getConnectorEx.getCause();
          handler.handle(Future.failedFuture(ex));
          return null;
        })
        .thenRun(() -> handler.handle(Future.succeededFuture(completableFutureList
            .stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList()))));
  }

  public static void getConnectors(RoutingContext context, String ownerId, Handler<AsyncResult<List<Connector>>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.getByOwner(marker, ownerId, ar -> {
      if (ar.failed()) {
        logger.warn(marker, "Unable to load resource definitions.'", ar.cause());
        handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", ar.cause())));
      } else {
        handler.handle(Future.succeededFuture(ar.result()));
      }
    });
  }

  public static void getAllConnectors(RoutingContext context, Handler<AsyncResult<List<Connector>>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.getAll(marker, ar -> {
      if (ar.failed()) {
        logger.warn(marker, "Unable to load resource definitions.'", ar.cause());
        handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", ar.cause())));
      } else {
        handler.handle(Future.succeededFuture(ar.result()));
      }
    });
  }

  public static void createConnector(RoutingContext context, JsonObject connector, Handler<AsyncResult<Connector>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.get(marker, connector.getString("id"), ar -> {
      if (ar.failed()) {
        storeConnector(context, connector, handler, marker, ar);
      }
      else {
        logger.info(marker, "Resource with the given ID already exists.");
        handler.handle(Future.failedFuture(new HttpException(BAD_REQUEST, "Resource with the given ID already exists.")));
      }
    });
  }

  protected static void storeConnector(RoutingContext context, JsonObject connector, Handler<AsyncResult<Connector>> handler, Marker marker,
      AsyncResult<Connector> ar) {
    Connector c = DatabindCodec.mapper().convertValue(connector, Connector.class);
    DiffMap diffMap = (DiffMap) Patcher.getDifference(new HashMap<>(), asMap(connector));

    try {
      if (diffMap == null) {
        handler.handle(Future.failedFuture(new HttpException(BAD_REQUEST, "Connector payload is empty.")));
      } else {
        ConnectorApi.ConnectorAuthorization.validateAdminChanges(context, diffMap);
        storeConnector(context, handler, marker, ar, c);
      }
    } catch (HttpException e) {
      handler.handle(Future.failedFuture(e));
    }
  }

  private static void storeConnector(RoutingContext context, Handler<AsyncResult<Connector>> handler, Marker marker,
      AsyncResult<Connector> ar, Connector c) throws HttpException {
    validate(context, c);

    Service.connectorConfigClient.store(marker, c, ar2 -> {
      if (ar2.failed()) {
        logger.error(marker, "Unable to store resource definition.'", ar.cause());
        handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to store the resource definition.", ar2.cause())));
      } else {
        handler.handle(Future.succeededFuture(ar2.result()));
      }
    });
  }

  public static void updateConnector(RoutingContext context, JsonObject connector, Handler<AsyncResult<Connector>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.get(marker, connector.getString("id"), ar -> {
      if (ar.failed()) {
        logger.error(marker, "Unable to load resource definition.'", ar.cause());
        handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "Unable to load the resource definition.", ar.cause())));
      } else {

        Connector oldConnector = ar.result();
        Map oldConnectorMap = asMap(oldConnector);
        DiffMap diffMap = (DiffMap) Patcher.calculateDifferenceOfPartialUpdate(oldConnectorMap, asMap(connector), null, true);
        try {
          if (diffMap == null) {
            handler.handle(Future.succeededFuture(oldConnector));
          } else {
            ConnectorApi.ConnectorAuthorization.validateAdminChanges(context, diffMap);
            Patcher.patch(oldConnectorMap, diffMap);
            Connector newHeadConnector = asConnector(marker, oldConnectorMap);
            storeConnector(context, handler, marker, ar, newHeadConnector);
          }
        } catch (HttpException e) {
          handler.handle(Future.failedFuture(e));
        }
      }
    });
  }

  public static void deleteConnector(RoutingContext context, String connectorId, Handler<AsyncResult<Connector>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.delete(marker, connectorId, ar -> {
      if (ar.failed()) {
        logger.error(marker, "Unable to delete resource definition.'", ar.cause());
        handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to delete the resource definition.", ar.cause())));
      } else {
        handler.handle(Future.succeededFuture(ar.result()));
      }
    });
  }

  private static Map asMap(Object object) {
    try {
      return DatabindCodec.mapper().convertValue(object, Map.class);
    }
    catch (Exception e) {
      return Collections.emptyMap();
    }
  }

  private static Connector asConnector(Marker marker, Map object) throws HttpException {
    try {
      return DatabindCodec.mapper().convertValue(object, Connector.class);
    }
    catch (Exception e) {
      logger.error(marker, "Could not convert resource.", e.getCause());
      throw new HttpException(INTERNAL_SERVER_ERROR, "Could not convert resource.");
    }
  }


  private static void validate(RoutingContext context, Connector connector) throws HttpException {
    //Validate general parameter
    if (connector.id == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'id' for the resource is missing.");
    else if (connector.id.length() < ID_MIN_LENGTH || connector.id.length() > ID_MAX_LENGTH)
        throw new HttpException(BAD_REQUEST, "Parameter 'id' must have a minimum length of  " + ID_MIN_LENGTH + " and a "
            + "maximum length of " + ID_MAX_LENGTH);

    if (connector.contactEmails == null || connector.contactEmails.isEmpty())
      throw new HttpException(BAD_REQUEST, "Parameter 'contactEmails' for the resource is missing or empty.");

    if (connector.owner == null)
      connector.owner = Api.Context.getJWT(context).aid;

    //Validate AWSLambdaRemoteFunction parameters
    Connector.RemoteFunctionConfig remoteFunctionConfig;
    try {
      remoteFunctionConfig = connector.getRemoteFunction();
    } catch (RuntimeException e) {
      throw new HttpException(BAD_REQUEST, "No 'remoteFunction' config provided.");
    }
    if (remoteFunctionConfig instanceof AWSLambda) {
      AWSLambda rf = (AWSLambda) remoteFunctionConfig;
      validateAWSLambda(connector, rf);
    } else if (remoteFunctionConfig instanceof Http) {
      Http rf = (Http) remoteFunctionConfig;
      validateHttp(connector, rf);
    } else if (remoteFunctionConfig instanceof Embedded) {
      Embedded rf = (Embedded) remoteFunctionConfig;
      validateEmbedded(connector, rf);
    } else {
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction' does not include a valid configuration.");
    }
  }

  private static void validateAWSLambda(Connector connector, AWSLambda rf) throws HttpException {
    //Validate RemoteFunction
    validateRfId(rf);

    if (rf.lambdaARN == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.lambdaARN' for the resource is missing.");
    else
    if (!rf.lambdaARN.matches("arn:aws:lambda:[a-z]{2}(-gov)?-[a-z]+-\\d{1}:\\d{12}:function:([a-zA-Z0-9-_]+)(:(\\$LATEST|[a-zA-Z0-9-_]+))?"))
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.lambdaARN' must be a valid Lambda function ARN.");

    if (rf.roleARN != null)
      if (!rf.roleARN.matches("arn:aws:iam::  \\d{12}:role/[a-zA-Z0-9_+=,.@-]+"))
        throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.roleARN' must be a valid IAM role ARN");

    if (rf.warmUp < 0 || rf.warmUp > 32)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.warmup' needs to be in the range of 0-32.");

    //Validate ConnectionSettings
    validateConnectionSettings(connector, 512);
    validateMaxPayload(connector, 6);
  }

  private static void validateHttp(Connector connector, Http rf) throws HttpException {
    //Validate RemoteFunction
    validateRfId(rf);

    if (rf.url == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.url' for the resource is missing.");
    else if (rf.url.toString().isEmpty())
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.url' for the resource is empty.");

    if (rf.warmUp != 0)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.warmup' must not be set for HTTP connectors.");

    //Validate ConnectionSettings
    validateConnectionSettings(connector, 5096);
    validateMaxPayload(connector, 50);
  }

  private static void validateRfId(RemoteFunctionConfig rf) throws HttpException {
    if (rf.id == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.id' for the resource is missing.");
    else if (rf.id.length() < ID_MIN_LENGTH || rf.id.length() > ID_MAX_LENGTH)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.id' must have a minimum length of  " + ID_MIN_LENGTH + " and a "
          + "maximum length of " + ID_MAX_LENGTH);
  }

  private static void validateEmbedded(Connector connector, Embedded rf) throws HttpException {
    //Validate RemoteFunction
    validateRfId(rf);

    if (rf.warmUp != 0)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.warmup' must not be set for Embedded connectors.");

    if (StringUtils.isEmpty(rf.className))
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.className' for the resource is missing.");
    try {
      Class<?> connectorClass = Class.forName(rf.className);
      if (!AbstractConnectorHandler.class.isAssignableFrom(connectorClass))
        throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.className' can't be resolved to a valid connector handler class.");
    }
    catch (ClassNotFoundException e) {
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.className' can't be resolved to a class.");
    }
    validateConnectionSettings(connector, 5096);
    validateMaxPayload(connector, 50);
  }

  private static void validateConnectionSettings(Connector connector, int maxConnections) throws HttpException {
    //Validate ConnectionSettings
    if (connector.connectionSettings.getMinConnections() < 0 || connector.connectionSettings.getMinConnections() > 256)
      throw new HttpException(BAD_REQUEST, "Parameter 'connectionSettings.minConnections' needs to be in the range of 0-256.");
    if (connector.connectionSettings.maxConnections < 16 || connector.connectionSettings.maxConnections > maxConnections)
      throw new HttpException(BAD_REQUEST, "Parameter 'connectionSettings.maxConnections' needs to be in the range of 16-" + maxConnections + ".");
  }

  private static void validateMaxPayload(Connector connector, int maxPayloadSize) throws HttpException {
    //Validate Capabilities
    if (connector.capabilities.maxPayloadSize < 0 || connector.capabilities.maxPayloadSize > maxPayloadSize * 1024 * 1024)
      throw new HttpException(BAD_REQUEST, "Parameter 'capabilities.maxPayloadSize' needs to be in the range of 0-" + (maxPayloadSize * 1024 * 1024) + " (default: " + (maxPayloadSize * 1024 * 1024) + ").");
  }
}
