/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.here.xyz.events.Event;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.connectors.models.Space.CacheProfile;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline.C1;
import com.here.xyz.hub.task.TaskPipeline.C2;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Marker;

/**
 * A task for processing of an event.
 */
public abstract class Task<T extends Event, X extends Task<T, ?>> {

  /**
   * The corresponding routing context.
   */
  public final RoutingContext context;

  /**
   * Describes, if cache should be ignored.
   */
  public final boolean skipCache;
  /**
   * The response type that should be produced by this task.
   */
  public ApiResponseType responseType;

  /**
   * Describes, if the response was loaded from cache.
   */
  private boolean cacheHit;

  /**
   * The event to process.
   */
  private T event;

  /**
   * Indicates, if the task was executed.
   */
  private boolean executed = false;

  /**
   * @throws NullPointerException if the given context or responseType are null.
   */
  public Task(T event, final RoutingContext context, final ApiResponseType responseType, boolean skipCache)
      throws NullPointerException {
    if (event == null) throw new NullPointerException("event");
    if (context == null) {
      throw new NullPointerException("context");
    }
    if (responseType == null) {
      throw new NullPointerException("responseType");
    }
    event.setIfNoneMatch(context.request().headers().get("If-None-Match"));
    this.event = event;
    this.context = context;
    this.responseType = responseType;
    this.skipCache = skipCache;
  }

  public T getEvent() {
    return event;
  }

  public String getCacheKey() {
    return null;
  }

  public boolean etagMatch() {
    return event.getIfNoneMatch() != null && event.getIfNoneMatch().equals(etag());
  }

  public void execute(C1<X> onSuccess, C2<X, Exception> onException) {
    if (!executed) {
      executed = true;
      getPipeline()
          .finish(onSuccess, onException)
          .execute();
    }
  }

  /**
   * Returns the log marker.
   *
   * @return the log marker
   */
  public Marker getMarker() {
    return Api.Context.getMarker(context);
  }

  /**
   * Returns the payload of the JWT Token.
   *
   * @return the payload of the JWT Token
   */
  public JWTPayload getJwt() {
    return Api.Context.getJWT(context);
  }

  /**
   * Returns the cache profile.
   *
   * @return the cache profile
   */
  public CacheProfile getCacheProfile() {
    return CacheProfile.NO_CACHE;
  }

  /**
   * Returns the execution pipeline.
   *
   * @return the execution pipeline
   */
  public abstract TaskPipeline<X> getPipeline();

  /**
   * Returns the e-tag of the response, if there is any available.
   *
   * @return the e-tag value.
   */
  public String etag() {
    return null;
  }

  public boolean isCacheHit() {
    return cacheHit;
  }

  public void setCacheHit(boolean cacheHit) {
    this.cacheHit = cacheHit;
  }
}
