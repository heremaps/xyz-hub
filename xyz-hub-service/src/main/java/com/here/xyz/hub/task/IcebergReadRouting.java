/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static com.here.xyz.hub.cache.QueryTimeoutCache.Decision.PROBE;
import static com.here.xyz.models.hub.IcebergLocation.EVENT_PARAM;
import static com.here.xyz.models.hub.IcebergLocation.EVENT_PARAM_LOCATION;
import static com.here.xyz.models.hub.IcebergLocation.EVENT_PARAM_MAX_RESPONSE_BYTES;
import static com.here.xyz.models.hub.IcebergLocation.ITERATE_TOKEN_PREFIX;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.XYZHubRESTVerticle;
import com.here.xyz.hub.config.IcebergVersionCache;
import com.here.xyz.hub.config.IcebergVersionCache.IcebergVersion;
import com.here.xyz.hub.connectors.RpcClient;
import com.here.xyz.hub.connectors.RpcClient.RpcContext;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.task.IcebergShadowComparison.Result;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * Routes eligible reads of versions, which were exported to Iceberg, to the Iceberg connector. Iceberg is a replica of the
 * immutable versions of a space: only reads of a specific version (or of the head of a read-only space) are routed, the
 * space's own connector stays the source of truth and is the fallback if the Iceberg connector fails or does not respond in
 * time. The mode of each space is its property {@code icebergReads}, else the Hub setting
 * {@link com.here.xyz.hub.Config#ICEBERG_READS}, see {@link IcebergReadMode}. The Iceberg versions of a space are only looked
 * up if its mode is not OFF, or for a page with an Iceberg token.
 * <p>
 * The table and tag of a read come from the {@code location} of the space's ICEBERG data reference, as stored. Nothing checks
 * that they belong to the space, so the references API must only be open to trusted callers.
 * <p>
 * The pages of an iterate served by the Iceberg connector carry tokens starting with
 * {@value IcebergLocation#ITERATE_TOKEN_PREFIX}. Such pages are always read from Iceberg, without a fallback.
 */
public final class IcebergReadRouting {

  private static final Logger logger = LogManager.getLogger();

  private static final Invoker DEFAULT_INVOKER = (task, connector, event, handler) -> RpcClient.getInstanceFor(connector)
      .execute(task.getMarker(), event, false, handler, task.space, task.getRequesterId());
  private static final Function<FeatureTask<?, ?>, Future<Connector>> DEFAULT_CONNECTOR_RESOLVER =
      task -> Service.connectorConfigClient.get(task.getMarker(), Service.configuration.ICEBERG_READS_CONNECTOR_ID);

  private static volatile Invoker invoker = DEFAULT_INVOKER;
  private static volatile Function<FeatureTask<?, ?>, Future<Connector>> connectorResolver = DEFAULT_CONNECTOR_RESOLVER;
  private static volatile IcebergVersionCache versionCache;
  private static volatile BiConsumer<String, Result> shadowListener;

  private static final ThreadPoolExecutor comparisons = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
      new ArrayBlockingQueue<>(16), runnable -> {
        Thread thread = new Thread(runnable, "iceberg-shadow-comparison");
        thread.setDaemon(true);
        return thread;
      });
  private static final LongAdder skippedComparisons = new LongAdder();
  /**
   * How long a missing or inactive Iceberg connector is remembered, so that it is not looked up for every read
   */
  static final long MISSING_CONNECTOR_TTL_MS = 30_000;
  private static volatile MissingConnector missingConnector;

  private IcebergReadRouting() {}

  /**
   * Runs an event on a connector, see {@link RpcClient#execute(Marker, Event, boolean, Handler, com.here.xyz.hub.connectors.models.Space,
   * String)}.
   */
  interface Invoker {

    RpcContext execute(FeatureTask<?, ?> task, Connector connector, Event<?> event, Handler<AsyncResult<XyzResponse>> handler)
        throws Exception;
  }

  /**
   * The pipeline step between readCache and invoke. It either sets the response of the task from the Iceberg connector, so that
   * invoke is skipped, or leaves the task to the space's own connector.
   */
  static <X extends FeatureTask<?, X>> void route(X task, Callback<X> callback) {
    //Cache hit
    if (task.getResponse() != null) {
      callback.call(task);
      return;
    }
    //invoke rejects the read if the space's connector is inactive
    if (task.storage != null && !task.storage.active) {
      callback.call(task);
      return;
    }

    Event<?> event = task.getEvent();
    String token = event instanceof IterateFeaturesEvent<?> iterate ? iterate.getNextPageToken() : null;
    boolean iceToken = token != null && token.startsWith(ITERATE_TOKEN_PREFIX);
    //A page token of the space's own connector
    if (token != null && !iceToken) {
      callback.call(task);
      return;
    }

    //The space's own mode, else the Hub setting; a composite has its own mode
    IcebergReadMode mode = IcebergReadMode.of(task.space);
    if (iceToken) {
      String reason = task.readOnlyAccess ? IcebergReadEligibility.reasonNotEligible(task) : "no immutable version";
      if (reason != null) {
        logger.info(task.getMarker(), "iceberg-read space={} rejected nextPageToken: {}", task.space.getId(), reason);
        callback.exception(new HttpException(BAD_REQUEST, "Invalid nextPageToken"));
        return;
      }
    }
    else {
      String skipReason = skipReason(task, mode);
      if (skipReason != null) {
        logger.debug(task.getMarker(), "iceberg-read space={} not routed: {}", task.space == null ? null : task.space.getId(),
            skipReason);
        callback.call(task);
        return;
      }
    }

    long version;
    try {
      version = ((ContextAwareEvent<?>) event).getRef().getVersion();
    }
    catch (Ref.InvalidRef e) {
      if (iceToken)
        callback.exception(new HttpException(BAD_REQUEST, "Invalid nextPageToken"));
      else
        callback.call(task);
      return;
    }

    if (!iceToken && mode == IcebergReadMode.SHADOW) {
      shadow(task, callback, version);
      return;
    }

    resolveConnector(task).onComplete(connectorResult -> {
      Connector connector = connectorResult.succeeded() ? connectorResult.result() : null;
      if (connector == null || !connector.active) {
        //resolveConnector logged it
        if (iceToken) {
          //Without the connector there are no valid Iceberg tokens, e.g. a crafted token while the routing is not configured
          logger.debug(task.getMarker(), "iceberg-read space={} rejected nextPageToken: The Iceberg connector {} is not available",
              task.space.getId(), Service.configuration.ICEBERG_READS_CONNECTOR_ID);
          callback.exception(new HttpException(BAD_REQUEST, "Invalid nextPageToken"));
        }
        else
          callback.call(task);
        return;
      }
      String filterReason = IcebergReadEligibility.reasonNotEligible(task, IcebergReadEligibility.filterProperties(connector));
      if (filterReason != null) {
        if (iceToken) {
          logger.info(task.getMarker(), "iceberg-read space={} rejected nextPageToken: {}", task.space.getId(), filterReason);
          callback.exception(new HttpException(BAD_REQUEST, "Invalid nextPageToken"));
        }
        else {
          logger.debug(task.getMarker(), "iceberg-read space={} not routed: {}", task.space.getId(), filterReason);
          callback.call(task);
        }
        return;
      }
      lookup(task, version).onComplete(lookupResult -> {
        if (lookupResult.failed()) {
          if (iceToken)
            callback.exception(new HttpException(BAD_GATEWAY, "Unable to resolve the nextPageToken.", lookupResult.cause()));
          else
            callback.call(task);
          return;
        }
        Optional<IcebergVersion> icebergVersion = lookupResult.result();
        if (icebergVersion.isEmpty()) {
          if (iceToken)
            callback.exception(new HttpException(BAD_REQUEST, "The nextPageToken is no longer valid"));
          else
            callback.call(task);
          return;
        }
        executeRouted(task, callback, connector, icebergVersion.get().location(), version, iceToken);
      });
    });
  }

  /**
   * Starts a shadow read for the task. The read of the space's own connector does not wait for the Iceberg connector and the
   * version lookup: if they are not resolved yet, the pipeline continues at once and the shadow read starts when they are.
   */
  private static <X extends FeatureTask<?, X>> void shadow(X task, Callback<X> callback, long version) {
    Future<Optional<Target>> target = resolveConnector(task).compose(connector -> {
      if (connector == null || !connector.active
          || IcebergReadEligibility.reasonNotEligible(task, IcebergReadEligibility.filterProperties(connector)) != null)
        return Future.succeededFuture(Optional.empty());
      return lookup(task, version).map(icebergVersion -> icebergVersion.map(v -> new Target(connector, v.location())));
    });
    if (target.isComplete()) {
      if (target.succeeded() && target.result().isPresent()) {
        ShadowRun shadow = newShadow(task, target.result().get().location(), version);
        Event<?> copy = copyEvent(task, shadow);
        if (copy != null) {
          task.icebergShadow = shadow;
          XYZHubRESTVerticle.addStreamInfo(task.context, "ISH", 1);
          shadow.start(task, target.result().get(), copy);
        }
      }
      callback.call(task);
      return;
    }

    //The event must be copied before invoke consumes it
    ShadowRun shadow = newShadow(task, null, version);
    Event<?> copy = copyEvent(task, shadow);
    if (copy == null) {
      callback.call(task);
      return;
    }
    task.icebergShadow = shadow;
    XYZHubRESTVerticle.addStreamInfo(task.context, "ISH", 1);
    callback.call(task);
    target.onComplete(result -> {
      if (result.succeeded() && result.result().isPresent())
        shadow.start(task, result.result().get(), copy);
      else
        shadow.abandon(result.failed() ? result.cause().getMessage() : "no Iceberg version");
    });
  }

  /**
   * @return The Iceberg connector, null if it is missing or inactive (then remembered for {@value #MISSING_CONNECTOR_TTL_MS} ms),
   *     or a failed future
   */
  private static Future<Connector> resolveConnector(FeatureTask<?, ?> task) {
    String connectorId = Service.configuration.ICEBERG_READS_CONNECTOR_ID;
    MissingConnector missing = missingConnector;
    if (missing != null && Objects.equals(missing.connectorId(), connectorId) && Core.currentTimeMillis() < missing.until())
      return Future.succeededFuture();
    Future<Connector> connector;
    try {
      connector = connectorResolver.apply(task);
    }
    catch (Exception e) {
      connector = Future.failedFuture(e);
    }
    return connector.transform(result -> {
      if (result.succeeded() && result.result() != null && result.result().active)
        return Future.succeededFuture(result.result());
      missingConnector = new MissingConnector(connectorId, Core.currentTimeMillis() + MISSING_CONNECTOR_TTL_MS);
      logger.warn(task.getMarker(), "iceberg-read: The Iceberg connector {} is not available, reads are not routed for {} ms",
          connectorId, MISSING_CONNECTOR_TTL_MS, result.cause());
      return Future.succeededFuture();
    });
  }

  /**
   * A missing or inactive Iceberg connector, until when it is not looked up again
   */
  private record MissingConnector(String connectorId, long until) {}

  /**
   * The pipeline step right after invoke. It hands the response of the space's own connector to a running shadow read.
   */
  static <X extends FeatureTask<?, X>> void recordPrimary(X task, Callback<X> callback) {
    ShadowRun shadow = task.icebergShadow;
    if (shadow != null) {
      task.icebergShadow = null;
      shadow.primaryDone(task.getResponse());
    }
    callback.call(task);
  }

  /**
   * @return Why the read must not be routed, or {@code null} if it can be routed if there is an Iceberg version for it
   */
  private static String skipReason(FeatureTask<?, ?> task, IcebergReadMode mode) {
    if (mode == IcebergReadMode.OFF)
      return "mode OFF";
    if (mode == IcebergReadMode.SHADOW && ThreadLocalRandom.current().nextDouble() >= Service.configuration.ICEBERG_READS_SHADOW_SAMPLE)
      return "not sampled";
    if (task.space == null)
      return "no space";
    String reason = IcebergReadEligibility.reasonNotEligible(task);
    if (reason != null)
      return reason;
    if (!task.readOnlyAccess)
      return "no immutable version";
    if (task.queryTimeoutLookup != null && task.queryTimeoutLookup.decision == PROBE)
      return "query timeout probe";
    return null;
  }

  private static Future<Optional<IcebergVersion>> lookup(FeatureTask<?, ?> task, long version) {
    try {
      IcebergVersionCache cache = versionCache != null ? versionCache : IcebergVersionCache.getInstance();
      return cache.lookup(task.getMarker(), task.space, version, IcebergReadEligibility.compositeFilter(task, version));
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  /**
   * @return A copy of the task's event for the Iceberg connector
   */
  private static Event<?> icebergEvent(FeatureTask<?, ?> task, Connector connector, IcebergLocation location) {
    Event<?> copy = task.getEvent().copy();
    if (copy == null)
      throw new IllegalStateException("Unable to copy the event");
    return withIcebergParams(task, connector, location, copy, null);
  }

  /**
   * Adds the location and the connector specific properties to a copy of the task's event.
   *
   * @param maxResponseBytes A lower limit for the size of the response, or null for the connector's own limit
   */
  private static Event<?> withIcebergParams(FeatureTask<?, ?> task, Connector connector, IcebergLocation location, Event<?> copy,
      Long maxResponseBytes) {
    Map<String, Object> params = new HashMap<>(copy.getParams() == null ? Map.of() : copy.getParams());
    params.put(EVENT_PARAM, maxResponseBytes == null ? Map.of(EVENT_PARAM_LOCATION, location.toString())
        : Map.of(EVENT_PARAM_LOCATION, location.toString(), EVENT_PARAM_MAX_RESPONSE_BYTES, maxResponseBytes));
    copy.setParams(params);
    FeatureTaskHandler.setAdditionalEventProps(task, connector, copy);
    return copy;
  }

  /**
   * Runs the event copy on the Iceberg connector with a timeout and calls the handler exactly once, with the response, the
   * failure or a timeout.
   */
  private static void execute(FeatureTask<?, ?> task, Connector connector, Event<?> event, boolean cancelWithTask,
      Handler<AsyncResult<XyzResponse>> handler) {
    AtomicBoolean done = new AtomicBoolean();
    AtomicReference<RpcContext> rpcContext = new AtomicReference<>();
    long timerId = Core.vertx.setTimer(Math.max(1, Service.configuration.ICEBERG_READS_TIMEOUT_MS), id -> {
      if (done.compareAndSet(false, true)) {
        cancel(rpcContext.get());
        handler.handle(Future.failedFuture(new HttpException(GATEWAY_TIMEOUT, "The Iceberg connector did not respond in time.")));
      }
    });
    Handler<AsyncResult<XyzResponse>> onceHandler = result -> {
      if (done.compareAndSet(false, true)) {
        Core.vertx.cancelTimer(timerId);
        handler.handle(result);
      }
    };
    try {
      RpcContext context = invoker.execute(task, connector, event, onceHandler);
      rpcContext.set(context);
      if (done.get())
        cancel(context);
      if (cancelWithTask)
        task.addCancellingHandler(t -> cancel(context));
    }
    catch (Exception e) {
      onceHandler.handle(Future.failedFuture(e));
    }
  }

  private static void cancel(RpcContext context) {
    if (context == null)
      return;
    try {
      context.cancelRequest();
    }
    catch (Exception e) {
      logger.debug("Unable to cancel the Iceberg read", e);
    }
  }

  private static <X extends FeatureTask<?, X>> void executeRouted(X task, Callback<X> callback, Connector connector,
      IcebergLocation location, long version, boolean iceToken) {
    String eventType = task.getEvent().getClass().getSimpleName();
    String spaceId = task.space.getId();
    long start = Core.currentTimeMillis();
    Event<?> event;
    try {
      event = icebergEvent(task, connector, location);
    }
    catch (Exception e) {
      onRoutedFailure(task, callback, e, eventType, version, location, start, iceToken);
      return;
    }
    execute(task, connector, event, true, result -> {
      if (task.getState().isFinal())
        return;
      if (result.succeeded() && result.result() instanceof FeatureCollection response) {
        long ms = Core.currentTimeMillis() - start;
        task.setResponse(response);
        task.queryTimeoutLookup = null;
        XYZHubRESTVerticle.addStreamInfo(task.context, "SID", connector.id);
        XYZHubRESTVerticle.addStreamInfo(task.context, "ITime", ms);
        logger.info(task.getMarker(), "iceberg-read space={} event={} version={} location={} ms={}", spaceId, eventType, version,
            location, ms);
        callback.call(task);
      }
      else {
        Throwable cause = result.failed() ? result.cause()
            : new IllegalStateException("Unexpected response " + (result.result() == null ? null
                : result.result().getClass().getSimpleName()));
        onRoutedFailure(task, callback, cause, eventType, version, location, start, iceToken);
      }
    });
  }

  private static <X extends FeatureTask<?, X>> void onRoutedFailure(X task, Callback<X> callback, Throwable cause, String eventType,
      long version, IcebergLocation location, long start, boolean iceToken) {
    long ms = Core.currentTimeMillis() - start;
    if (iceToken) {
      logger.warn(task.getMarker(), "iceberg-read failed space={} event={} version={} location={} ms={}", task.space.getId(),
          eventType, version, location, ms, cause);
      callback.exception(cause instanceof Exception ? cause : new HttpException(GATEWAY_TIMEOUT, "The Iceberg read failed.", cause));
      return;
    }
    XYZHubRESTVerticle.addStreamInfo(task.context, "IFB", 1);
    logger.warn(task.getMarker(), "iceberg-read fallback space={} event={} version={} location={} ms={}", task.space.getId(),
        eventType, version, location, ms, cause);
    //invoke runs the original event on the space's own connector
    callback.call(task);
  }

  private static ShadowRun newShadow(FeatureTask<?, ?> task, IcebergLocation location, long version) {
    Event<?> event = task.getEvent();
    return new ShadowRun(task.getMarker(), task.space.getId(), event.getClass().getSimpleName(), location, version,
        event instanceof SearchForFeaturesEvent<?> search ? search.getLimit() : 0, task.context.request().uri());
  }

  /**
   * @return A copy of the task's event, or null if it can not be copied
   */
  private static Event<?> copyEvent(FeatureTask<?, ?> task, ShadowRun shadow) {
    try {
      Event<?> copy = task.getEvent().copy();
      if (copy == null)
        throw new IllegalStateException("Unable to copy the event");
      return copy;
    }
    catch (Exception e) {
      logger.warn(task.getMarker(), "iceberg-shadow space={}: Unable to start the shadow read", shadow.spaceId, e);
      return null;
    }
  }

  /**
   * The Iceberg connector and the location of the version to read
   */
  private record Target(Connector connector, IcebergLocation location) {}

  private static void report(String result, Result comparison) {
    BiConsumer<String, Result> listener = shadowListener;
    if (listener != null)
      listener.accept(result, comparison);
  }

  /**
   * A read of the Iceberg connector which runs next to the read of the space's own connector. Once both are done, the responses
   * are compared in the background and the outcome is logged.
   */
  static final class ShadowRun {

    private final Marker marker;
    private final String spaceId;
    private final String eventType;
    private volatile IcebergLocation location;
    private final long version;
    private final long limit;
    private final String uri;
    private final long maxChars = Math.max(0, Service.configuration.ICEBERG_READS_SHADOW_MAX_BYTES);
    private final long start = Core.currentTimeMillis();
    private boolean tooLarge;
    private boolean primaryDone;
    private XyzResponse<?> primary;
    private long primaryMs;
    private AsyncResult<XyzResponse> iceberg;
    private long icebergMs;
    private boolean completed;
    private boolean abandoned;

    private ShadowRun(Marker marker, String spaceId, String eventType, IcebergLocation location, long version, long limit,
        String uri) {
      this.marker = marker;
      this.spaceId = spaceId;
      this.eventType = eventType;
      this.location = location;
      this.version = version;
      this.limit = limit;
      this.uri = uri;
    }

    /**
     * Starts the Iceberg read with the copy of the task's event.
     */
    void start(FeatureTask<?, ?> task, Target target, Event<?> copy) {
      location = target.location();
      Event<?> icebergEvent;
      try {
        //The Iceberg connector fails fast instead of building a response which would not be compared
        icebergEvent = withIcebergParams(task, target.connector(), target.location(), copy, maxChars);
      }
      catch (Exception e) {
        logger.warn(marker, "iceberg-shadow space={}: Unable to start the shadow read", spaceId, e);
        abandon(e.getMessage());
        return;
      }
      execute(task, target.connector(), icebergEvent, false, this::icebergDone);
    }

    /**
     * Ends the shadow read without an Iceberg read, e.g. because there is no Iceberg version.
     */
    void abandon(String reason) {
      synchronized (this) {
        abandoned = true;
        primary = null;
      }
      logger.debug(marker, "iceberg-shadow space={} event={} not started: {}", spaceId, eventType, reason);
    }

    void primaryDone(XyzResponse<?> response) {
      //Not kept while the Iceberg read runs, and not handed to the comparison, if it is too large to be compared
      boolean large = IcebergShadowComparison.isLargerThan(response, maxChars);
      synchronized (this) {
        if (abandoned)
          return;
        primaryDone = true;
        primary = large ? null : response;
        tooLarge |= large;
        primaryMs = Core.currentTimeMillis() - start;
      }
      completeIfDone();
    }

    void icebergDone(AsyncResult<XyzResponse> result) {
      boolean large = result.succeeded() && IcebergShadowComparison.isLargerThan(result.result(), maxChars);
      synchronized (this) {
        iceberg = large ? Future.succeededFuture() : result;
        tooLarge |= large;
        icebergMs = Core.currentTimeMillis() - start;
      }
      completeIfDone();
    }

    private void completeIfDone() {
      XyzResponse<?> primaryResponse;
      AsyncResult<XyzResponse> icebergResult;
      boolean skip;
      synchronized (this) {
        if (!primaryDone || iceberg == null || completed || abandoned)
          return;
        completed = true;
        primaryResponse = primary;
        icebergResult = iceberg;
        skip = tooLarge;
        primary = null;
        iceberg = null;
      }
      if (skip) {
        logger.info(marker, "iceberg-shadow space={} event={} version={} result=SKIPPED reason={} pgMs={} iceMs={} location={}",
            spaceId, eventType, version, IcebergShadowComparison.TOO_LARGE, primaryMs, icebergMs, location);
        report("SKIPPED", IcebergShadowComparison.Result.skipped(IcebergShadowComparison.TOO_LARGE));
        return;
      }
      if (icebergResult.failed()) {
        logger.info(marker, "iceberg-shadow space={} event={} result=ERROR reason={} pgMs={} iceMs={} location={}", spaceId, eventType,
            icebergResult.cause().getMessage(), primaryMs, icebergMs, location);
        report("ERROR", null);
        return;
      }
      try {
        comparisons.execute(() -> compare(primaryResponse, icebergResult.result()));
      }
      catch (RejectedExecutionException e) {
        skippedComparisons.increment();
        logger.info(marker, "iceberg-shadow space={} event={} result=SKIPPED reason=skipped busy skipped={} location={}", spaceId,
            eventType, skippedComparisons.sum(), location);
        report("SKIPPED", null);
      }
    }

    private void compare(XyzResponse<?> primaryResponse, XyzResponse<?> icebergResponse) {
      try {
        Result result = IcebergShadowComparison.compare(primaryResponse, icebergResponse, limit, maxChars);
        String message = "iceberg-shadow space={} event={} version={} result={} reason={} pgMs={} iceMs={} pgCount={} iceCount={} "
            + "missing={} extra={} differing={} location={}";
        if (result.outcome() == IcebergShadowComparison.Outcome.MISMATCH)
          logger.warn(marker, message + " uri={}", spaceId, eventType, version, result.outcome(), result.reason(), primaryMs,
              icebergMs, result.primaryCount(), result.icebergCount(), result.missing(), result.extra(), result.differing(),
              location, uri);
        else
          logger.info(marker, message, spaceId, eventType, version, result.outcome(), result.reason(), primaryMs, icebergMs,
              result.primaryCount(), result.icebergCount(), result.missing(), result.extra(), result.differing(), location);
        report(result.outcome().name(), result);
      }
      catch (Exception e) {
        logger.warn(marker, "iceberg-shadow space={} event={}: The comparison failed", spaceId, eventType, e);
        report("ERROR", null);
      }
    }
  }

  //Test seams

  static void setInvoker(Invoker testInvoker) {
    invoker = testInvoker;
  }

  static void setConnectorResolver(Function<FeatureTask<?, ?>, Future<Connector>> testResolver) {
    connectorResolver = testResolver;
  }

  static void setVersionCache(IcebergVersionCache testCache) {
    versionCache = testCache;
  }

  static void setShadowListener(BiConsumer<String, Result> listener) {
    shadowListener = listener;
  }

  static void resetTestSeams() {
    invoker = DEFAULT_INVOKER;
    connectorResolver = DEFAULT_CONNECTOR_RESOLVER;
    versionCache = null;
    shadowListener = null;
    missingConnector = null;
  }
}
