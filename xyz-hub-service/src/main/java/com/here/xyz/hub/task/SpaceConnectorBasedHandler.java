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

package com.here.xyz.hub.task;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
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
    return SpaceConfigClient.getInstance().get(marker, e.getSpace())
      .flatMap(space -> space == null
          ? Future.failedFuture(new HttpException(BAD_REQUEST, "The resource ID '" + e.getSpace() + "' does not exist!"))
          : Future.succeededFuture(space))
      .flatMap(authorizationFunction)
      .flatMap(space -> {
                    injectEventParameters(e, space);
                    return Future.succeededFuture(space);
              }
      )
      .flatMap(space -> {
                    try {
                        validateBasedOnSpaceConfig(e, space);
                        return Future.succeededFuture(space);
                    }catch (HttpException ex){
                        return Future.failedFuture(ex);
                    }
                }
            )
      .flatMap(space -> ConnectorConfigClient.getInstance().get(marker, space.getStorage().getId()))
      .map(RpcClient::getInstanceFor)
      .recover(t -> t instanceof HttpException
          ? Future.failedFuture(t)
          : Future.failedFuture(new HttpException(BAD_REQUEST, "Connector is not available", t)))
      .flatMap(rpcClient -> {
        final Promise<R> promise = Promise.promise();
        rpcClient.execute(marker, e, handler -> {
          if (handler.failed())
            promise.fail(new HttpException(BAD_REQUEST, "Error calling the connector"));
          else
            promise.complete((R) handler.result());
        });
        return promise.future();
      });
  }

  private static void injectEventParameters(Event e, Space space){
      if (e instanceof GetChangesetStatisticsEvent) {
          ((GetChangesetStatisticsEvent) e).setMinSpaceVersion(space.getMinVersion());
          ((GetChangesetStatisticsEvent) e).setVersionsToKeep(space.getVersionsToKeep());
      }else if (e instanceof IterateChangesetsEvent) {
          ((IterateChangesetsEvent) e).setMinSpaceVersion(space.getMinVersion());
          ((IterateChangesetsEvent) e).setVersionsToKeep(space.getVersionsToKeep());
      }
  }

  private static void validateBasedOnSpaceConfig(Event e, Space space) throws HttpException {
        if (e instanceof IterateChangesetsEvent) {
            if(((IterateChangesetsEvent) e).getStartVersion() < space.getMinVersion()) {
                if(((IterateChangesetsEvent) e).getEndVersion() == null)
                    throw new HttpException(NOT_FOUND, "The requested version '"+((IterateChangesetsEvent) e).getStartVersion()+"' got deleted.");
                else
                    throw new HttpException(BAD_REQUEST, "StartVersion is to low! Min startVersion = "+space.getMinVersion());
            }
        }
    }
}
