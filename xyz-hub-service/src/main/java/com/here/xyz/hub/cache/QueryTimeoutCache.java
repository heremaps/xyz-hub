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
import static com.here.xyz.util.service.rest.Api.RESPONSE_PAYLOAD_TOO_LARGE;
import static com.here.xyz.util.service.rest.Api.RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.StatementTimeoutException;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import io.vertx.core.Future;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Remembers queries whose statement timed out in the database repeatedly, or whose response was too large, so that a client retrying the
 * same query gets the same 504 or 513 without the query running again for each attempt.
 *
 * <p>The marker of a query is kept in Redis, because the retries of a client spread over the service nodes. Its key is the cache key of
 * the request, so {@code skipCache=true}, non-cacheable spaces and content updates bypass it in the same way as the response cache. The
 * value is the kind of the last failure, the number of failures with the same status in a row and the time the last one ended.
 *
 * <ul>
 *   <li>Only statement timeouts ({@link StatementTimeoutException}) and too large responses (513, see {@link Outcome}) are counted. A
 *   request which started before the last counted failure with the same status ended is not counted again.</li>
 *   <li>From {@link #QUERY_TIMEOUT_CACHE_THRESHOLD} failures on, requests get the same reply without running the query. After
 *   {@link #QUERY_TIMEOUT_CACHE_PROBE_AFTER} seconds since the last failure, the next request runs the query again (probe).</li>
 *   <li>A query on a version which cannot change any more (see {@link #lookUp(String, String, boolean)}) gives the same answer each time.
 *   After its first 513, requests get the 513 without running the query until the marker expires
 *   {@link #QUERY_TIMEOUT_CACHE_IMMUTABLE_TTL} seconds later. Statement timeouts of such a query are counted as above.</li>
 *   <li>A success removes the marker, as does any other server error. Client errors (4xx, incl. 429) leave it as it is.</li>
 *   <li>For {@link #QUERY_TIMEOUT_CACHE_GUARD_HOLD} seconds after any other timeout of a connector, e.g., the connector not answering
 *   in time during a database outage, this node neither counts nor serves markers for that connector.</li>
 * </ul>
 *
 * A lost marker (Redis error or eviction) only means that the query runs again.
 */
public class QueryTimeoutCache {

  private static final Logger logger = LogManager.getLogger();
  private static final String KEY_PREFIX = "QUERY_TIMEOUT:";

  /**
   * The number of failures with the same status in a row of the same query after which the reply is served without running the query.
   */
  public static final int QUERY_TIMEOUT_CACHE_THRESHOLD = 3;
  /**
   * The lifetime of a marker in seconds. A marker expires that long after the last failure of its query.
   */
  public static final int QUERY_TIMEOUT_CACHE_TTL = 600; //seconds
  /**
   * The lifetime in seconds of the marker of a too large response of a query on a version which cannot change any more. Such a query
   * gets the 513 without running for that long after its last 513, without probes in between.
   */
  public static final int QUERY_TIMEOUT_CACHE_IMMUTABLE_TTL = (int) HOURS.toSeconds(6);
  /**
   * The time in seconds after the last failure of a query after which the next request runs the query again (probe).
   * A probe that succeeds removes the marker. A probe that fails in the same way again restarts the period.
   */
  public static final int QUERY_TIMEOUT_CACHE_PROBE_AFTER = 300; //seconds
  /**
   * The time in seconds for which a service node neither records nor serves markers of a connector after that connector had any
   * other timeout (e.g., the connector did not answer in time).
   */
  public static final int QUERY_TIMEOUT_CACHE_GUARD_HOLD = 600; //seconds

  private final CacheClient markers;
  private final LongSupplier clock;

  /**
   * The time of the last timeout of each connector which was not a statement timeout, as seen by this node. The key is the connector ID.
   */
  private final ConcurrentHashMap<String, Long> otherTimeouts = new ConcurrentHashMap<>();

  public enum Decision {
    /**
     * Run the query.
     */
    RUN,
    /**
     * Run the query, which failed repeatedly before.
     */
    PROBE,
    /**
     * Reply with the failure without running the query.
     */
    SHORT_CUT
  }

  /**
   * The failures of a query which are counted.
   */
  public enum Outcome {
    /**
     * The database cancelled the query, 504 (E318540).
     */
    STATEMENT_TIMEOUT(null, GATEWAY_TIMEOUT.code()),
    /**
     * The connector refused to build a response that large ({@code PAYLOAD_TO_LARGE}), 513 (E318541).
     */
    CONNECTOR_TOO_LARGE("C", RESPONSE_PAYLOAD_TOO_LARGE.code()),
    /**
     * The response of the connector exceeded the size limits of the service, 513 ({@code RESPONSE_PAYLOAD_TOO_LARGE}).
     */
    RESPONSE_TOO_LARGE("R", RESPONSE_PAYLOAD_TOO_LARGE.code());

    /**
     * The prefix of the marker value. Statement timeout markers keep the format they had before 513s were counted.
     */
    private final String prefix;
    public final int status;

    Outcome(String prefix, int status) {
      this.prefix = prefix;
      this.status = status;
    }

    /**
     * @return The counted outcome of the failure of a query, or null if the failure is not counted
     */
    static Outcome of(Throwable cause) {
      if (cause instanceof StatementTimeoutException)
        return STATEMENT_TIMEOUT;
      if (cause instanceof DetailedHttpException e)
        return "E318541".equals(e.errorDefinition.getCode()) ? CONNECTOR_TOO_LARGE : null;
      if (cause instanceof HttpException e && e.status.code() == RESPONSE_PAYLOAD_TOO_LARGE.code()
          && RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE.equals(e.getMessage()))
        return RESPONSE_TOO_LARGE;
      return null;
    }

    boolean isTooLarge() {
      return status == RESPONSE_PAYLOAD_TOO_LARGE.code();
    }

    /**
     * @return A new exception with the same reply as the real failure
     */
    HttpException toException() {
      return switch (this) {
        case STATEMENT_TIMEOUT -> new DetailedHttpException("E318540");
        case CONNECTOR_TOO_LARGE -> new DetailedHttpException("E318541");
        case RESPONSE_TOO_LARGE -> new HttpException(RESPONSE_PAYLOAD_TOO_LARGE, RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE);
      };
    }

    private static Outcome ofPrefix(String prefix) {
      for (Outcome outcome : values())
        if (prefix.equals(outcome.prefix))
          return outcome;
      return null;
    }
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
   * @param immutable Whether the query reads a version which cannot change any more, so that a too large response stays too large
   * @return The lookup to be passed to {@link #onSuccess(Lookup)} or {@link #onFailure(Lookup, Throwable)} once the query has run, or null
   *  if the query should neither be counted nor be short-cut
   */
  public Future<Lookup> lookUp(String cacheKey, String connectorId, boolean immutable) {
    if (!isEnabled() || isPaused(connectorId))
      return Future.succeededFuture(null);

    final String key = KEY_PREFIX + cacheKey;
    return markers.get(key)
        .otherwise((byte[]) null)
        .map(value -> new Lookup(key, connectorId, immutable, clock.getAsLong(), QueryMarker.fromBytes(value)));
  }

  /**
   * Is called after the query of a lookup returned a response. Removes the marker, if there was one.
   */
  public void onSuccess(Lookup lookup) {
    if (lookup.marker != null)
      markers.remove(lookup.key);
  }

  /**
   * Is called after the query of a lookup failed. Counts statement timeouts and too large responses, removes the marker on other server
   * errors.
   */
  public void onFailure(Lookup lookup, Throwable cause) {
    Outcome outcome = Outcome.of(cause);
    if (outcome != null) {
      if (!isPaused(lookup.connectorId))
        count(lookup, outcome);
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
          QUERY_TIMEOUT_CACHE_GUARD_HOLD);
  }

  private void count(Lookup lookup, Outcome outcome) {
    //Read the marker again, as requests of the same query on other nodes may have changed it while this one was running
    markers.get(lookup.key)
        .otherwise((byte[]) null)
        .onSuccess(value -> {
          QueryMarker current = QueryMarker.fromBytes(value);
          //A failure with another status starts a new series
          boolean sameStatus = current != null && current.outcome.status == outcome.status;
          //Another request of the same query failed in the same way while this one was running, count overlapping requests once
          if (sameStatus && current.lastFailureEnd > lookup.startedAt)
            return;
          int count = sameStatus ? current.count + 1 : 1;
          long ttl = lookup.immutable && outcome.isTooLarge() ? QUERY_TIMEOUT_CACHE_IMMUTABLE_TTL : QUERY_TIMEOUT_CACHE_TTL;
          markers.set(lookup.key, new QueryMarker(outcome, count, clock.getAsLong()).toBytes(), ttl);
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
    return SECONDS.toMillis(QUERY_TIMEOUT_CACHE_GUARD_HOLD);
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
     * The time in milliseconds since the last counted failure of the query, or -1 if there was no marker.
     */
    public final long markerAge;
    private final String key;
    private final String connectorId;
    private final boolean immutable;
    private final long startedAt;
    private final QueryMarker marker;

    private Lookup(String key, String connectorId, boolean immutable, long startedAt, QueryMarker marker) {
      this.key = key;
      this.connectorId = connectorId;
      this.immutable = immutable;
      this.startedAt = startedAt;
      this.marker = marker;
      markerAge = marker == null ? -1 : startedAt - marker.lastFailureEnd;
      decision = decide();
    }

    private Decision decide() {
      if (marker == null)
        return RUN;
      //The same version gives the same answer, no need to wait for more 513s or to probe before the marker expires
      if (immutable && marker.outcome.isTooLarge())
        return SHORT_CUT;
      if (marker.count < QUERY_TIMEOUT_CACHE_THRESHOLD)
        return RUN;
      return markerAge >= SECONDS.toMillis(QUERY_TIMEOUT_CACHE_PROBE_AFTER) ? PROBE : SHORT_CUT;
    }

    /**
     * @return The status of the remembered failure, or -1 if there was no marker
     */
    public int status() {
      return marker == null ? -1 : marker.outcome.status;
    }

    /**
     * @return A new exception with the same reply as the remembered failure, to be sent for a {@link Decision#SHORT_CUT}
     */
    public HttpException shortCutReply() {
      return marker.outcome.toException();
    }
  }

  /**
   * The value of a marker. A statement timeout is stored as {@code <count>:<lastFailureEnd>}, the format from before 513s were counted,
   * the others as {@code <prefix>:<count>:<lastFailureEnd>}. A service node of an older version cannot read the latter and runs the query.
   */
  private record QueryMarker(Outcome outcome, int count, long lastFailureEnd) {

    private static QueryMarker fromBytes(byte[] value) {
      if (value == null)
        return null;
      try {
        String[] parts = new String(value, UTF_8).split(":");
        if (parts.length == 2)
          return new QueryMarker(Outcome.STATEMENT_TIMEOUT, Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
        Outcome outcome = parts.length == 3 ? Outcome.ofPrefix(parts[0]) : null;
        return outcome == null ? null : new QueryMarker(outcome, Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
      }
      catch (RuntimeException e) {
        //Treat an unreadable marker as no marker
        return null;
      }
    }

    private byte[] toBytes() {
      String value = count + ":" + lastFailureEnd;
      return (outcome.prefix == null ? value : outcome.prefix + ":" + value).getBytes(UTF_8);
    }
  }
}
