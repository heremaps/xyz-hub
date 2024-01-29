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

package com.here.xyz.hub.task;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;

import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.responses.XyzResponse;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class SpaceConnectorBasedHandler {
  private static final Logger logger = LogManager.getLogger();

  public static <T extends Event<T>, R extends XyzResponse<R>> Future<R> execute(Marker marker, Function<Space, Future<Space>> authorizationFunction, Event<T> e) {
    return getAndValidateSpace(marker, e.getSpace())
      .compose(authorizationFunction)
      .compose(space -> injectEventParameters(marker, e, space))
      .compose(space -> Service.connectorConfigClient.get(marker, space.getStorage().getId()))
      .map(RpcClient::getInstanceFor)
      .recover(t -> t instanceof HttpException
          ? Future.failedFuture(t)
          : Future.failedFuture(new HttpException(BAD_REQUEST, "Connector is not available", t)))
      .compose(rpcClient -> {
        final Promise<R> promise = Promise.promise();
        rpcClient.execute(marker, e, handler -> {
          if (handler.failed()) {
            logger.warn(marker, "Error during rpc execution for event: " + e.getClass(), handler.cause());
            promise.fail(new HttpException(BAD_REQUEST, handler.cause().getMessage()));
          }
          else
            promise.complete((R) handler.result());
        });
        return promise.future();
      });
  }

  private static Future<Space> injectEventParameters(Marker marker, Event e, Space space){
      Promise<Space> p = Promise.promise();

      if (e instanceof GetChangesetStatisticsEvent || e instanceof DeleteChangesetsEvent || e instanceof IterateChangesetsEvent) {
          if(e instanceof DeleteChangesetsEvent || e instanceof  GetChangesetStatisticsEvent) {
              getMinTag(marker, space.getId())
                      .onSuccess(minTag -> {
                          if (e instanceof DeleteChangesetsEvent)
                              ((DeleteChangesetsEvent) e).setMinTagVersion(minTag);
                          else if (e instanceof GetChangesetStatisticsEvent) {
                              ((GetChangesetStatisticsEvent) e).setVersionsToKeep(space.getVersionsToKeep());
                              ((GetChangesetStatisticsEvent) e).setMinSpaceVersion(space.getMinVersion());
                              ((GetChangesetStatisticsEvent) e).setMinTagVersion(minTag);
                          }
                          p.complete(space);
                      }).onFailure(
                              t -> p.fail(new HttpException(BAD_GATEWAY, "Unexpected problem!"))
                      );
          }else{
              ((IterateChangesetsEvent) e).setVersionsToKeep(space.getVersionsToKeep());
              p.complete(space);
          }
      }else
          p.complete(space);
      return p.future();
  }

  private static Future<Long> getMinTag(Marker marker, String space){
      return Service.tagConfigClient.getTags(marker,space)
            .compose(r -> r == null ? Future.succeededFuture(null)
                    : Future.succeededFuture(r.stream().mapToLong(tag -> tag.getVersion()).min())
            )
            .compose(r -> {
                /* Return min tag of this space */
                return Future.succeededFuture((r.isPresent() ? r.getAsLong() : null));
            });
  }

  public static Future<Space> getAndValidateSpace(Marker marker, String spaceId) {
    return Service.spaceConfigClient.get(marker, spaceId)
        .compose(space -> space == null
            ? Future.failedFuture(new HttpException(BAD_REQUEST, "The resource ID '" + spaceId + "' does not exist!"))
            : Future.succeededFuture(space))
        .compose(space -> !space.isActive()
            ? Future.failedFuture(new HttpException(METHOD_NOT_ALLOWED, "The method is not allowed, because the resource \"" + spaceId + "\" is not active."))
            : Future.succeededFuture(space));
  }
}
