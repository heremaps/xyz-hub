package com.here.xyz.hub.task;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.AWSLambda;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.hub.util.diff.Difference.DiffMap;
import com.here.xyz.hub.util.diff.Difference.Primitive;
import com.here.xyz.hub.util.diff.Patcher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class ConnectorHandler {

  private static final Logger logger = LogManager.getLogger();

  public static void getConnector(RoutingContext context, String connectorId, Handler<AsyncResult<Connector>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.get(marker, connectorId, ar -> {
      if (ar.failed()) {
        logger.error(marker, "The requested resource does not exist.'", ar.cause());
        handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "The requested resource does not exist.", ar.cause())));
      } else {
        handler.handle(Future.succeededFuture(ar.result()));
      }
    });
  }

  public static void getConnectors(RoutingContext context, String ownerId, Handler<AsyncResult<List<Connector>>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.getByOwner(marker, ownerId, ar -> {
      if (ar.failed()) {
        logger.error(marker, "Unable to load resource definitions.'", ar.cause());
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

        Connector c = Json.mapper.convertValue(connector, Connector.class);
        DiffMap diffMap = (DiffMap) Patcher.getDifference(new HashMap<>(), asMap(connector));
        try {
          validateAdminChanges(context, diffMap);
          validate(context, c);

          Service.connectorConfigClient.store(marker, c, ar2 -> {
            if (ar2.failed()) {
              logger.error(marker, "Unable to store resource definition.'", ar.cause());
              handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to store the resource definition.", ar2.cause())));
            } else {
              handler.handle(Future.succeededFuture(ar2.result()));
            }
          });
        } catch (HttpException e) {
          handler.handle(Future.failedFuture(e));
        }
      } else {
        logger.info(marker, "Resource with the given ID already exists.");
        handler.handle(Future.failedFuture(new HttpException(BAD_REQUEST, "Resource with the given ID already exists.")));
      }
    });
  }

  public static void replaceConnector(RoutingContext context, JsonObject connector, Handler<AsyncResult<Connector>> handler) {
    Marker marker = Api.Context.getMarker(context);

    Service.connectorConfigClient.get(marker, connector.getString("id"), ar -> {
      if (ar.failed()) {
        logger.error(marker, "Unable to load resource definition.'", ar.cause());
        handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "Unable to load the resource definition.", ar.cause())));
      } else {

        Connector c = Json.mapper.convertValue(connector, Connector.class);
        DiffMap diffMap = (DiffMap) Patcher.getDifference(new HashMap<>(), asMap(connector));
        try {
          validateAdminChanges(context, diffMap);
          validate(context, c);

          Service.connectorConfigClient.store(marker, c, ar2 -> {
            if (ar2.failed()) {
              logger.error(marker, "Unable to store resource definition.'", ar.cause());
              handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to store the resource definition.", ar2.cause())));
            } else {
              handler.handle(Future.succeededFuture(ar2.result()));
            }
          });
        } catch (HttpException e) {
          handler.handle(Future.failedFuture(e));
        }
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
          validateAdminChanges(context, diffMap);
          Patcher.patch(oldConnectorMap, diffMap);
          Connector newHeadConnector = asConnector(marker, oldConnectorMap);
          validate(context, newHeadConnector);

          Service.connectorConfigClient.store(marker, newHeadConnector, ar2 -> {
            if (ar2.failed()) {
              logger.error(marker, "Unable to store resource definition.'", ar.cause());
              handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to store the resource definition.", ar2.cause())));
            } else {
              handler.handle(Future.succeededFuture(ar2.result()));
            }
          });
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
      return Json.decodeValue(Json.mapper.writeValueAsString(object), Map.class);
    } catch (Exception e) {
      return Collections.emptyMap();
    }
  }

  private static Connector asConnector(Marker marker, Map object) throws HttpException {
    try {
      return Json.mapper.convertValue(object, Connector.class);
    } catch (Exception e) {
      logger.error(marker, "Could not convert resource.", e.getCause());
      throw new HttpException(INTERNAL_SERVER_ERROR, "Could not convert resource.");
    }
  }

  private static void validateAdminChanges(RoutingContext context, DiffMap diffMap) throws HttpException {
    //Is Admin change?
    checkParameterChange(diffMap.get("owner"), "owner", Api.Context.getJWT(context).aid);
    checkParameterChange(diffMap.get("skipAutoDisable"), "skipAutoDisable", false);
    checkParameterChange(diffMap.get("trusted"), "trusted", false);
  }

  private static void checkParameterChange(Difference diff, String parameterName, Object expected) throws HttpException {
    if (diff != null && diff instanceof Primitive) {
      Primitive prim = (Primitive) diff;
      if (prim.newValue() != null && !prim.newValue().equals(expected)) throw new HttpException(FORBIDDEN, "The property '" + parameterName + "' can not be changed manually.");
    }
  }

  private static void validate(RoutingContext context, Connector connector) throws HttpException {
    //Validate general parameter
    if (connector.id == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'id' for the resource is missing.");
    else
      if (!connector.id.matches("[a-z0-9-_]{4,64}"))
        throw new HttpException(BAD_REQUEST, "Parameter 'id' needs to match [a-z0-9-_]{4,64}.");

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
    if (rf.id == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.id' for the resource is missing.");
    else
    if (!rf.id.matches("[a-z0-9-_]{4,64}"))
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.id' needs to match [a-z0-9-_]{4,64}.");

    if (rf.lambdaARN == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.lambdaARN' for the resource is missing.");
    else
    if (!rf.lambdaARN.matches("arn:aws:lambda:[a-z]{2}(-gov)?-[a-z]+-\\d{1}:\\d{12}:function:([a-zA-Z0-9-_]+)(:(\\$LATEST|[a-zA-Z0-9-_]+))?"))
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.lambdaARN' needs to match arn:aws:lambda:[a-z]{2}(-gov)?-[a-z]+-\\d{1}:\\d{12}:function:([a-zA-Z0-9-_]+)(:(\\$LATEST|[a-zA-Z0-9-_]+))?.");

    if (rf.roleARN != null)
      if (!rf.roleARN.matches("arn:aws:iam::  \\d{12}:role/[a-zA-Z0-9_+=,.@-]+"))
        throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.roleARN' needs to match arn:aws:iam::\\d{12}:role/[a-zA-Z0-9_+=,.@-]+");

    if (rf.warmUp < 0 || rf.warmUp > 32)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.warmup' needs to be in the range of 0-32.");


    //Validate ConnectionSettings
    if (connector.connectionSettings.getMinConnections() < 0 || connector.connectionSettings.getMinConnections() > 256)
      throw new HttpException(BAD_REQUEST, "Parameter 'connectionSettings.minConnections' needs to be in the range of 0-256.");
    if (connector.connectionSettings.maxConnections < 16 || connector.connectionSettings.maxConnections > 2048)
      throw new HttpException(BAD_REQUEST, "Parameter 'connectionSettings.maxConnections' needs to be in the range of 16-2048.");

    //Validate Capabilities
    if (connector.capabilities.maxPayloadSize < 0 || connector.capabilities.maxPayloadSize > 6 * 1024 * 1024)
      throw new HttpException(BAD_REQUEST, "Parameter 'capabilities.maxPayloadSize' needs to be in the range of 0-6291456 (default: 6291456).");
  }

  private static void validateHttp(Connector connector, Http rf) throws HttpException {
    //Validate RemoteFunction
    if (rf.id == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.id' for the resource is missing.");
    else
    if (!rf.id.matches("[a-z0-9-_]{4,64}"))
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.id' needs to match [a-z0-9-_]{4,64}.");

    if (rf.url == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.url' for the resource is missing.");
    else if (rf.url.toString().isEmpty())
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.url' for the resource is empty.");

    if (rf.warmUp < 0 || rf.warmUp > 32)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.warmup' needs to be in the range of 0-32.");


    //Validate ConnectionSettings
    if (connector.connectionSettings.getMinConnections() < 0 || connector.connectionSettings.getMinConnections() > 256)
      throw new HttpException(BAD_REQUEST, "Parameter 'connectionSettings.minConnections' needs to be in the range of 0-256.");
    if (connector.connectionSettings.maxConnections < 16 || connector.connectionSettings.maxConnections > 5096)
      throw new HttpException(BAD_REQUEST, "Parameter 'connectionSettings.maxConnections' needs to be in the range of 16-5096.");

    //Validate Capabilities
    if (connector.capabilities.maxPayloadSize < 0 || connector.capabilities.maxPayloadSize > 50 * 1024 * 1024)
      throw new HttpException(BAD_REQUEST, "Parameter 'capabilities.maxPayloadSize' needs to be in the range of 0-52.428.800 (default: 52.428.800).");
  }

  private static void validateEmbedded(Connector connector, Embedded rf) throws HttpException {
    //Validate RemoteFunction
    if (rf.id == null)
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.id' for the resource is missing.");
    else
    if (!rf.id.matches("[a-z0-9-_]{4,64}"))
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.id' needs to match [a-z0-9-_]{4,64}.");

    if (StringUtils.isEmpty(rf.className))
      throw new HttpException(BAD_REQUEST, "Parameter 'remoteFunction.className' for the resource is missing.");


    //Validate ConnectionSettings
    if (connector.connectionSettings.getMinConnections() < 0 || connector.connectionSettings.getMinConnections() > 256)
      throw new HttpException(BAD_REQUEST, "Parameter 'connectionSettings.minConnections' needs to be in the range of 0-256.");
    if (connector.connectionSettings.maxConnections < 16 || connector.connectionSettings.maxConnections > 5096)
      throw new HttpException(BAD_REQUEST, "Parameter 'connectionSettings.maxConnections' needs to be in the range of 16-5096.");

    //Validate Capabilities
    if (connector.capabilities.maxPayloadSize < 0 || connector.capabilities.maxPayloadSize > 50 * 1024 * 1024)
      throw new HttpException(BAD_REQUEST, "Parameter 'capabilities.maxPayloadSize' needs to be in the range of 0-52.428.800 (default: 52.428.800).");
  }
}
