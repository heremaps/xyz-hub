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

package com.here.xyz.hub.cache;

import static com.here.xyz.hub.cache.QueryTimeoutCache.Decision.PROBE;
import static com.here.xyz.hub.cache.QueryTimeoutCache.Decision.RUN;
import static com.here.xyz.hub.cache.QueryTimeoutCache.Decision.SHORT_CUT;
import static com.here.xyz.hub.cache.QueryTimeoutCache.TtlMode.SLIDING;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.StatementTimeoutException;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Remembers queries whose statement timed out in the database repeatedly, so that a client retrying the same query gets the same 504
 * without the query running again for each attempt.
 *
 * <p>The marker of a query is kept in Redis, because the retries of a client spread over the service nodes. Its key is the cache key of
 * the request, so {@code skipCache=true}, non-cacheable spaces and content updates bypass it in the same way as the response cache. The
 * value is the number of statement timeouts in a row and the time the last one ended.
 *
 * <ul>
 *   <li>Only statement timeouts ({@link StatementTimeoutException}) are counted. A request which started before the last counted timeout
 *   ended is not counted again.</li>
 *   <li>From {@link Config#QUERY_TIMEOUT_CACHE_THRESHOLD} timeouts on, requests get the 504 without running the query. After
 *   {@link Config#QUERY_TIMEOUT_CACHE_PROBE_AFTER} seconds since the last timeout, the next request runs the query again (probe).</li>
 *   <li>A success removes the marker, as does any other server error. Client errors (4xx, incl. 429) leave it as it is.</li>
 *   <li>For {@link Config#QUERY_TIMEOUT_CACHE_GUARD_HOLD} seconds after any other timeout of a connector, e.g., the connector not answering
 *   in time during a database outage, this node neither counts nor serves markers for that connector.</li>
 * </ul>
 *
 * A lost marker (Redis error or eviction) only means that the query runs again.
 */
public class QueryTimeoutCache {

  private static final Logger logger = LogManager.getLogger();
  private static final String KEY_PREFIX = "QUERY_TIMEOUT:";

  private final CacheClient markers;
  private final LongSupplier clock;

  /**
   * The time of the last timeout of each connector which was not a statement timeout, as seen by this node. The key is the connector ID.
   */
  private final ConcurrentHashMap<String, Long> otherTimeouts = new ConcurrentHashMap<>();

  public enum TtlMode {
    /**
     * A marker expires {@link Config#QUERY_TIMEOUT_CACHE_TTL} seconds after the last statement timeout.
     */
    FIXED,
    /**
     * Each reply served from a marker renews it for {@link Config#QUERY_TIMEOUT_CACHE_TTL} seconds.
     */
    SLIDING
  }

  public enum Decision {
    /**
     * Run the query.
     */
    RUN,
    /**
     * Run the query, which timed out repeatedly before.
     */
    PROBE,
    /**
     * Reply with the timeout without running the query.
     */
    SHORT_CUT
  }

  /**
   * @param markers The cache client for the markers. It must not keep its own copies of the values, as a node would not see the removal of
   *  a marker by another node then.
   * @param clock The clock to measure the age of the markers
   */
  public QueryTimeoutCache(CacheClient markers, LongSupplier clock) {
    this.markers = markers;
    this.clock = clock;
  }

  public boolean isEnabled() {
    return config() != null && config().QUERY_TIMEOUT_CACHE_ENABLED;
  }

  /**
   * Reads the marker of a query before it runs.
   *
   * @param cacheKey The cache key of the request
   * @param connectorId The ID of the storage connector to run the query
   * @return The lookup to be passed to {@link #onSuccess(Lookup)} or {@link #onFailure(Lookup, Throwable)} once the query has run, or null
   *  if the query should neither be counted nor be short-cut
   */
  public Future<Lookup> lookUp(String cacheKey, String connectorId) {
    if (!isEnabled() || isPaused(connectorId))
      return Future.succeededFuture(null);

    final String key = KEY_PREFIX + cacheKey;
    return markers.get(key)
        .otherwise((byte[]) null)
        .map(value -> {
          Lookup lookup = new Lookup(key, connectorId, clock.getAsLong(), TimeoutMarker.fromBytes(value));
          if (lookup.decision == SHORT_CUT && config().QUERY_TIMEOUT_CACHE_TTL_MODE == SLIDING)
            markers.set(key, value, config().QUERY_TIMEOUT_CACHE_TTL);
          return lookup;
        });
  }

  /**
   * Is called after the query of a lookup returned a response. Removes the marker, if there was one.
   */
  public void onSuccess(Lookup lookup) {
    if (lookup.marker != null)
      markers.remove(lookup.key);
  }

  /**
   * Is called after the query of a lookup failed. Counts statement timeouts, removes the marker on other server errors.
   */
  public void onFailure(Lookup lookup, Throwable cause) {
    if (cause instanceof StatementTimeoutException) {
      if (!isPaused(lookup.connectorId))
        countTimeout(lookup);
    }
    else if (isServerError(cause) && lookup.marker != null)
      markers.remove(lookup.key);
  }

  /**
   * Is called for each failed call of a connector. Any timeout which is not a statement timeout pauses this cache for the connector on
   * this node.
   */
  public void onConnectorFailure(String connectorId, Throwable cause) {
    if (!isEnabled() || connectorId == null || !isOtherTimeout(cause))
      return;

    long now = clock.getAsLong();
    Long previous = otherTimeouts.put(connectorId, now);
    if (previous == null || now - previous >= guardHoldMillis())
      logger.info("Connector {} did not respond in time, pausing the query timeout cache for it for {} s on this node.", connectorId,
          config().QUERY_TIMEOUT_CACHE_GUARD_HOLD);
  }

  private void countTimeout(Lookup lookup) {
    //Read the marker again, as requests of the same query on other nodes may have changed it while this one was running
    markers.get(lookup.key)
        .otherwise((byte[]) null)
        .onSuccess(value -> {
          TimeoutMarker current = TimeoutMarker.fromBytes(value);
          //Another request of the same query timed out while this one was running, count overlapping requests once
          if (current != null && current.lastTimeoutEnd > lookup.startedAt)
            return;
          int count = current == null ? 1 : current.count + 1;
          markers.set(lookup.key, new TimeoutMarker(count, clock.getAsLong()).toBytes(), config().QUERY_TIMEOUT_CACHE_TTL);
        });
  }

  private boolean isPaused(String connectorId) {
    Long otherTimeout = connectorId == null ? null : otherTimeouts.get(connectorId);
    return otherTimeout != null && clock.getAsLong() - otherTimeout < guardHoldMillis();
  }

  private static boolean isOtherTimeout(Throwable cause) {
    return cause instanceof HttpException e && e.status.code() == GATEWAY_TIMEOUT.code() && !(cause instanceof StatementTimeoutException);
  }

  private static boolean isServerError(Throwable cause) {
    return !(cause instanceof HttpException e) || e.status.code() >= 500;
  }

  private static long guardHoldMillis() {
    return SECONDS.toMillis(config().QUERY_TIMEOUT_CACHE_GUARD_HOLD);
  }

  private static Config config() {
    return Service.configuration;
  }

  /**
   * The state of the marker of a query as read before the query ran.
   */
  public static final class Lookup {

    public final Decision decision;
    /**
     * The time in milliseconds since the last statement timeout of the query, or -1 if there was no marker.
     */
    public final long markerAge;
    private final String key;
    private final String connectorId;
    private final long startedAt;
    private final TimeoutMarker marker;

    private Lookup(String key, String connectorId, long startedAt, TimeoutMarker marker) {
      this.key = key;
      this.connectorId = connectorId;
      this.startedAt = startedAt;
      this.marker = marker;
      markerAge = marker == null ? -1 : startedAt - marker.lastTimeoutEnd;
      decision = decide();
    }

    private Decision decide() {
      if (marker == null || marker.count < config().QUERY_TIMEOUT_CACHE_THRESHOLD)
        return RUN;
      long probeAfter = SECONDS.toMillis(config().QUERY_TIMEOUT_CACHE_PROBE_AFTER);
      return probeAfter > 0 && markerAge >= probeAfter ? PROBE : SHORT_CUT;
    }
  }

  private record TimeoutMarker(int count, long lastTimeoutEnd) {

    private static TimeoutMarker fromBytes(byte[] value) {
      if (value == null)
        return null;
      try {
        String[] parts = new String(value, UTF_8).split(":");
        return new TimeoutMarker(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
      }
      catch (RuntimeException e) {
        //Treat an unreadable marker as no marker
        return null;
      }
    }

    private byte[] toBytes() {
      return (count + ":" + lastTimeoutEnd).getBytes(UTF_8);
    }
  }
}
