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

import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.task.SubscriptionHandler;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.here.xyz.hub.auth.XyzHubAttributeMap.SPACE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class SubscriptionApi extends Api {
  private static final Logger logger = LogManager.getLogger();

  public SubscriptionApi(RouterBuilder rb) {
    rb.operation("getSubscriptions").handler(this::getSubscriptions);
    rb.operation("postSubscription").handler(this::postSubscription);
    rb.operation("getSubscription").handler(this::getSubscription);
    rb.operation("putSubscription").handler(this::putSubscription);
    rb.operation("deleteSubscription").handler(this::deleteSubscription);
  }

  private Subscription getSubscriptionInput(final RoutingContext context) throws HttpException {
    try {
      return DatabindCodec.mapper().convertValue(context.getBodyAsJson(), Subscription.class);
    }
    catch (DecodeException e) {
      throw new HttpException(BAD_REQUEST, "Invalid JSON string");
    }
  }

  private void getSubscription(final RoutingContext context) {
    try {
      String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
      String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);

      SubscriptionApi.SubscriptionAuthorization.authorizeManageSpacesRights(context, spaceId, arAuth -> {
        if (arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
        } else {
          SubscriptionHandler.getSubscription(context, spaceId,subscriptionId, ar -> {
            if(ar.failed()) {
              sendErrorResponse(context, ar.cause());
            } else {
              sendResponse(context, OK, ar.result());
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
    String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
    SubscriptionAuthorization.authorizeManageSpacesRights(context, spaceId, arAuth -> {
      if(arAuth.failed()) {
        sendErrorResponse(context, arAuth.cause());
      } else {
        SubscriptionHandler.getSubscriptions(context, spaceId, ar -> {
          if(ar.failed()) {
            sendErrorResponse(context, ar.cause());
          } else {
            sendResponse(context, OK, ar.result());
          }
        });
      }
    });
  }

  private void postSubscription(final RoutingContext context) {
    try {
      String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
      Subscription subscription = getSubscriptionInput(context);
      subscription.setSource(spaceId);
      validateSubscriptionRequest(subscription);

      SubscriptionAuthorization.authorizeManageSpacesRights(context, spaceId, arAuth -> {
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

  private void putSubscription(final RoutingContext context) {
    try {
      String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
      String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);
      Subscription subscription = getSubscriptionInput(context);
      subscription.setId(subscriptionId);
      subscription.setSource(spaceId);
      validateSubscriptionRequest(subscription);

      SubscriptionAuthorization.authorizeManageSpacesRights(context, spaceId, arAuth -> {
        if(arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
        } else {
          SubscriptionHandler.createOrReplaceSubscription(context, subscription, ar -> {
            if(ar.failed()) {
              sendErrorResponse(context, ar.cause());
            } else {
              sendResponse(context, OK, ar.result());
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
      String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
      String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);

      // Check authorization
      SubscriptionApi.SubscriptionAuthorization.authorizeManageSpacesRights(context, spaceId, arAuth -> {
        if (arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
        } else {
          // Check if subscription exists
          SubscriptionHandler.getSubscription(context, spaceId, subscriptionId, arGet -> {
            if(arGet.failed()) {
              sendErrorResponse(context, arGet.cause());
            } else {
              Subscription subscription = arGet.result();
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

  }

}
