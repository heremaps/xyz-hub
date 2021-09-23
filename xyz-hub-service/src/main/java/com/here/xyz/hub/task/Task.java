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

import static com.here.xyz.hub.task.Task.TaskState.CANCELLED;
import static com.here.xyz.hub.task.Task.TaskState.ERROR;
import static com.here.xyz.hub.task.Task.TaskState.INIT;
import static com.here.xyz.hub.task.Task.TaskState.IN_PROGRESS;
import static com.here.xyz.hub.task.Task.TaskState.RESPONSE_SENT;
import static com.here.xyz.hub.task.Task.TaskState.STARTED;

import com.here.xyz.events.Event;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.connectors.models.Space.CacheProfile;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.TaskPipeline.C1;
import com.here.xyz.hub.task.TaskPipeline.C2;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.responses.XyzResponse;
import io.netty.util.internal.ConcurrentSet;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * A task for processing of an event.
 */
public abstract class Task<T extends Event, X extends Task<T, ?>> {

  public static final String TASK = "task";
  private static final Logger logger = LogManager.getLogger();

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
   * A local copy of {@link Event#getIfNoneMatch()}.
   */
  private String ifNoneMatch;

  /**
   * Whether the event was finally consumed.
   * After having been consumed the event get's deleted from memory and neither {@link #getEvent()} nor {@link #consumeEvent()} may
   * be called anymore. Otherwise an {@link IllegalStateException} will be thrown.
   */
  private boolean eventConsumed;

  /**
   * Indicates, if the task was executed.
   */
  private boolean executed = false;

  /**
   * The current state / phase of this task. The state can be read to know whether an action should still be performed or may be cancelled.
   * E.g. if the task is in a final state already it doesn't make sense to send a(nother) response or fail with another exception.
   *
   * @see TaskState
   */
  private TaskState state = INIT;

  private TaskPipeline<X> pipeline;

  private ConcurrentSet<Consumer<Task<T, X>>> cancellingHandlers = new ConcurrentSet<>();

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
    this.ifNoneMatch = event.getIfNoneMatch();
    this.context = context;
    context.put(TASK, this);
    this.responseType = responseType;
    this.skipCache = skipCache;
  }

  public T getEvent() throws IllegalStateException {
    if (eventConsumed) throw new IllegalStateException("Event was already consumed.");
    return event;
  }

  /**
   * Finally consumes the event. Calling this method the event being bound to this task will be returned and all internal references are
   * deleted. After the event has been been consumed neither {@link #getEvent()} nor {@link #consumeEvent()} may be called anymore.
   * Otherwise an {@link IllegalStateException} will be thrown.
   *
   * @throws IllegalStateException In case the event was consumed already
   */
  public T consumeEvent() throws IllegalStateException {
    T event = getEvent();
    eventConsumed = true;
    this.event = null;
    state = IN_PROGRESS;
    return event;
  }

  protected <X extends Task<?, X>> void cleanup(X task, Callback<X> callback) {}

  public String getCacheKey() {
    return null;
  }

  public boolean etagMatches() {
    return XyzResponse.etagMatches(ifNoneMatch, getEtag());
  }

  public void execute(C1<X> onSuccess, C2<X, Throwable> onException) {
    if (!executed) {
      executed = true;
      getPipeline()
          .finish(
              a -> {
                if (state.isFinal()) return;
                onSuccess.call(a);
                state = RESPONSE_SENT;
              },
              (a, b) -> {
                if (state.isFinal()) return;
                state = ERROR;
                onException.call(a, b);
              }
          )
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
   * Creates the execution pipeline.
   *
   * @return the execution pipeline
   */
  public abstract TaskPipeline<X> createPipeline();

  /**
   * Returns the (previously) created execution pipeline.
   * @return
   */
  public TaskPipeline<X> getPipeline() {
    if (pipeline == null)
      pipeline = createPipeline();

    return pipeline;
  }

  /**
   * Returns the e-tag of the response, if there is any available.
   *
   * @return the e-tag value.
   */
  public String getEtag() {
    return null;
  }

  public boolean isCacheHit() {
    return cacheHit;
  }

  public void setCacheHit(boolean cacheHit) {
    this.cacheHit = cacheHit;
  }

  public TaskState getState() {
    if (state == INIT && executed) return STARTED;
    return state;
  }

  public void addCancellingHandler(Consumer<Task<T, X>> cancellingHandler) {
    Objects.requireNonNull(cancellingHandler);
    cancellingHandlers.add(cancellingHandler);
  }

  /**
   * The state can be read to know whether an action should still be performed or may be cancelled.
   * E.g. when the task is in a final state already, it doesn't make sense to send a(nother) response or fail with another exception.
   *
   * Final states are: {@link TaskState#RESPONSE_SENT}
   * The following is true for final states:
   *  No further action should be started and pending actions should be cancelled / killed.
   *  If cancelling of some asynchronous action is not possible the action's handler should do nothing in case it still gets called.
   */
  public enum TaskState {

    /**
     * Init is the first state right after the task has been created. The execution of the task has not started yet.
     */
    INIT,

    /**
     * The execution of the task has been started by the {@link TaskPipeline}.
     */
    STARTED,

    /**
     * The main action of this task is in progress. This could be a running request the system is waiting for to succeed or fail.
     */
    IN_PROGRESS,

    /**
     * The task's actions have been performed and the response has been sent to the client.
     * This is a final state.
     */
    RESPONSE_SENT,

    /**
     * The task's execution has been cancelled. E.g. due to the request has been cancelled by the client.
     * This is a final state.
     */
    CANCELLED,

    /**
     * An error happened during the execution of one of this task's actions. This could happen either during the request- or response-phase.
     * This is a final state.
     */
    ERROR;

    public boolean isFinal() {
      return this == RESPONSE_SENT || this == CANCELLED || this == ERROR;
    }
  }

  public void cancel() {
    if (!state.isFinal()) {
      try {
        //Cancel all further steps in the pipeline
        getPipeline().cancel();
        /*
        Call all registered CancellingHandlers
        (e.g. to cancel running / pending requests which might have been started by previous actions already)
         */
        callCancellingHandlers();
      }
      catch (Exception e) {
        logger.error(getMarker(), "Error cancelling the task.", e);
      }
      finally {
        state = CANCELLED;
      }
    }
  }

  private void callCancellingHandlers() {
    cancellingHandlers.forEach(cH -> cH.accept(this));
  }
}
