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

package com.here.xyz.hub.rest;

import com.here.xyz.hub.auth.AttributeMap;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.task.SubscriptionHandler;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.here.xyz.hub.auth.XyzHubAttributeMap.SPACE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class SubscriptionApi extends Api {
  private static final Logger logger = LogManager.getLogger();

  public SubscriptionApi(RouterBuilder rb) {
    rb.operation("getSubscriptions").handler(this::getSubscriptions);
    rb.operation("postSubscription").handler(this::postSubscription);
    rb.operation("getSubscription").handler(this::getSubscription);
    rb.operation("deleteSubscription").handler(this::deleteSubscription);
  }

  private JsonObject getInput(final RoutingContext context) throws HttpException {
    try {
      return context.getBodyAsJson();
    }
    catch (DecodeException e) {
      throw new HttpException(BAD_REQUEST, "Invalid JSON string");
    }
  }

  private void getSubscription(final RoutingContext context) {
    try {
      String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);
      SubscriptionHandler.getSubscription(context, subscriptionId, ar -> {
        if(ar.failed()) {
          this.sendErrorResponse(context, ar.cause());
        } else {
          Subscription subscription = ar.result();
          SubscriptionApi.SubscriptionAuthorization.authorizeManageSpacesRights(context, subscription.getSource(), arAuth -> {
            if (arAuth.failed()) {
              sendErrorResponse(context, arAuth.cause());
            } else {
              sendResponse(context, OK, subscription);
            }
          });
        }
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void getSubscriptions(final RoutingContext context) {
    String sourceSpace = context.queryParams().get("source");
    if(sourceSpace == null) {
      if(SubscriptionAuthorization.isAdmin(context)) {
        SubscriptionHandler.getAllSubscriptions(context, ar -> {
          if (ar.failed()) {
            sendErrorResponse(context, ar.cause());
          } else {
            sendResponse(context, OK, ar.result());
          }
        });
      } else {
        sendErrorResponse(context, new HttpException(BAD_REQUEST, "The subscription source should be provided as query parameter"));
      }
    } else {
      SubscriptionAuthorization.authorizeManageSpacesRights(context, sourceSpace, arAuth -> {
        if(arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
        } else {
          SubscriptionHandler.getSubscriptions(context, sourceSpace, ar -> {
            if(ar.failed()) {
              sendErrorResponse(context, ar.cause());
            } else {
              sendResponse(context, OK, ar.result());
            }
          });
        }
      });
    }
  }

  private void postSubscription(final RoutingContext context) {
    try {
      Subscription subscription = DatabindCodec.mapper().convertValue(getInput(context), Subscription.class);
      validateSubscriptionRequest(subscription);

      SubscriptionAuthorization.authorizeManageSpacesRights(context, subscription.getSource(), arAuth -> {
        if(arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
        } else {
          SubscriptionHandler.createSubscription(context, subscription, ar -> {
            if(ar.failed()) {
              sendErrorResponse(context, ar.cause());
            } else {
              sendResponse(context, CREATED, ar.result());
            }
          });
        }
      });
    } catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void deleteSubscription(final RoutingContext context) {
    try {
      String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);
      // Check if subscription exists
      SubscriptionHandler.getSubscription(context, subscriptionId, arGet -> {
        if(arGet.failed()) {
          sendErrorResponse(context, arGet.cause());
        } else {
          Subscription subscription = arGet.result();
          // Check authorization
          SubscriptionApi.SubscriptionAuthorization.authorizeManageSpacesRights(context, subscription.getSource(), arAuth -> {
            if (arAuth.failed()) {
              sendErrorResponse(context, arAuth.cause());
            } else {
              // Delete subscription
              SubscriptionHandler.deleteSubscription(context, subscription, ar -> {
                if(ar.failed()) {
                  sendErrorResponse(context, ar.cause());
                } else {
                  sendResponse(context, OK, ar.result());
                }
              });
            }
          });
        }
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void validateSubscriptionRequest(Subscription subscription) throws HttpException {
    if(subscription.getId() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'id' cannot be empty.");
    if(subscription.getSource() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'source' cannot be empty.");
    if(subscription.getDestination() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'destination' cannot be empty.");
    if(subscription.getDestinationType() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'destinationType' cannot be empty.");
    if(subscription.getConfig() == null || subscription.getConfig().getType() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property config 'type' cannot be empty.");
  }

  public static class SubscriptionAuthorization extends Authorization {

    public static void authorizeManageSpacesRights(RoutingContext context, String spaceId, Handler<AsyncResult<Void>> handler) {
      final XyzHubActionMatrix requestRights = new XyzHubActionMatrix()
              .manageSpaces(new XyzHubAttributeMap().withValue(SPACE, spaceId));
      try {
        evaluateRights(Context.getMarker(context), requestRights, Context.getJWT(context).getXyzHubMatrix());
        handler.handle(Future.succeededFuture());
      } catch (HttpException e) {
        handler.handle(Future.failedFuture(e));
      }
    }

    private static boolean isAdmin(RoutingContext context) {
      XyzHubActionMatrix xyzHubActionMatrix = Api.Context.getJWT(context).getXyzHubMatrix();
      if (xyzHubActionMatrix == null) return false;
      List<AttributeMap> manageSpacesRights = xyzHubActionMatrix.get(XyzHubActionMatrix.MANAGE_SPACES);
      if (manageSpacesRights != null) {
        for (AttributeMap attributeMap : manageSpacesRights) {
          //If manageSpaces[{}] -> Admin (no restrictions)
          if (attributeMap.isEmpty()) {
            return true;
          }
        }
      }
      return false;
    }

  }

}
