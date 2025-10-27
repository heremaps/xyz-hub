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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.task.FeatureTask.resolveBranchFor;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class SpaceConnectorBasedHandler {
    private static final Logger logger = LogManager.getLogger();

    public static <T extends Event<T>, R extends XyzResponse<R>> Future<R> execute(
            Marker marker,
            Function<Space, Future<Space>> authorizationFunction,
            Event<T> event
    ) {
        return getAndValidateSpace(marker, event.getSpace())
                .compose(authorizationFunction)
                .compose(space -> injectEventParameters(marker, event, space)
                        .map(updatedSpace -> updatedSpace) // still have `space` in scope
                )
                .compose(space ->
                        Service.connectorConfigClient.get(marker, space.getStorage().getId())
                                .map(RpcClient::getInstanceFor)
                                .map(rpcClient -> new ExecutionContext<>(space, rpcClient, event))
                )
                .recover(t -> {
                    if (t instanceof HttpException) return Future.failedFuture(t);
                    return Future.failedFuture(new HttpException(BAD_REQUEST, "Connector is not available", t));
                })
                .compose(ctx -> {
                    final Promise<R> promise = Promise.promise();
                    ctx.rpcClient.execute(marker, ctx.event, handler -> {
                        if (handler.failed()) {
                            logger.warn(marker, "Error during rpc execution for event: " + event.getClass(), handler.cause());
                            if (handler.cause() instanceof HttpException)
                                promise.fail(handler.cause());
                            else
                                promise.fail(new HttpException(BAD_REQUEST, handler.cause().getMessage()));
                        } else {
                            R result = (R) handler.result();

                            if (result instanceof ChangesetsStatisticsResponse statsResponse
                                    && ctx.event instanceof GetChangesetStatisticsEvent) {
                                long minVersion = ctx.space.getMinVersion();

                                if (minVersion != 0L) {
                                    //FIXME: Only to be BWC - should be removed in future versions
                                    // if a minVersion is present it should used for override (else part)
                                    if(statsResponse.getMinTagVersion() != null
                                            && minVersion > statsResponse.getMinTagVersion())
                                        statsResponse.setMinVersion( statsResponse.getMinTagVersion());
                                    else
                                        statsResponse.setMinVersion(minVersion);
                                }
                            }

                            promise.complete(result);
                        }
                    });
                    return promise.future();
                });
    }

    private static class ExecutionContext<T extends Event<T>> {
        public Space space;
        public RpcClient rpcClient;
        public Event<T> event;

        public ExecutionContext(Space space, RpcClient rpcClient, Event<T> event) {
            this.space = space;
            this.rpcClient = rpcClient;
            this.event = event;
        }
    }

    private static Future<Space> injectEventParameters(Marker marker, Event event, Space space) {
      if (event instanceof DeleteChangesetsEvent || event instanceof GetChangesetStatisticsEvent || event instanceof IterateChangesetsEvent) {
        return getMinTag(marker, space.getId())
            .compose(minTag -> {
              if (event instanceof DeleteChangesetsEvent deleteChangesetsEvent && minTag != null) {
                //FIXME: getMinUserTag and use this version. Currently we use minTag to be BWC
                if (!minTag.isSystem() && minTag.getVersion() < deleteChangesetsEvent.getMinVersion())
                  return Future.failedFuture(new HttpException(BAD_REQUEST, "Tag \"" + minTag.getId() + "\"for version "
                      + minTag.getVersion() + " exists!"));
                else
                  deleteChangesetsEvent.setMinVersion(Math.min(minTag.getVersion(), deleteChangesetsEvent.getMinVersion()));
              }
              else if (event instanceof GetChangesetStatisticsEvent getChangesetStatisticsEvent && minTag != null) {
                //TODO: check minSpaceVersion
                getChangesetStatisticsEvent.setMinTagVersion(minTag.getVersion());
                getChangesetStatisticsEvent.setMinVersion(Math.min(minTag.getVersion(), space.getMinVersion()));
              }
              else if (event instanceof IterateChangesetsEvent iterateChangesetsEvent) {

                int v2k = space.getVersionsToKeep();

                if( v2k <= 1 )
                 return Future.failedFuture(new HttpException(BAD_REQUEST,"Changesets needs history enabled. VersionsToKeep should be greater 1"));

                iterateChangesetsEvent.setVersionsToKeep(v2k);

                if (minTag != null)
                  iterateChangesetsEvent.setMinVersion(minTag.getVersion());
              }
              return Future.succeededFuture();
            })
            .compose(v -> event instanceof ContextAwareEvent<?> contextAwareEvent
                    ? resolveBranchFor(contextAwareEvent, space) : Future.succeededFuture())
            .map(space);
      }
      else
        return Future.succeededFuture(space);
    }

    public static Future<Tag> getMinTag(Marker marker, String space) {
        return Service.tagConfigClient.getTags(marker, space, true)
                .compose(tags -> {
                    if (tags == null || tags.isEmpty()) {
                        return Future.succeededFuture(null);
                    }

                    // Find the tag with the minimum version
                    Tag minTag = tags.stream()
                            .min(Comparator.comparingLong(Tag::getVersion))
                            .orElse(null);

                    return Future.succeededFuture(minTag);
                });
    }

    public static Future<Space> getAndValidateSpace(Marker marker, String spaceId) {
        return Service.spaceConfigClient.get(marker, spaceId)
                .compose(space -> space == null
                        ? Future.failedFuture(new DetailedHttpException("E318441", Map.of("resourceId", spaceId)))
                        : Future.succeededFuture(space))
                .compose(space -> space != null && !space.isActive()
                        ? Future.failedFuture(new DetailedHttpException("E318451", Map.of("resourceId", spaceId)))
                        : Future.succeededFuture(space));
    }
}
