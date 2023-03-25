package com.here.xyz.hub.rest;

import com.here.xyz.NanoTime;
import com.here.xyz.events.Event;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.Task;
import com.here.xyz.hub.util.logging.AccessLog;
import io.vertx.core.MultiMap;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class Context {

  private static final String START_NANOS = "startNanos";
  private static final String STREAM_ID = "Stream-Id";
  private static final String ACCESS_LOG = "accessLog";
  private static final String JWT = "jwt";
  private static final String QUERY_PARAMS = "queryParams";
  private static final String TASK = "task";

  /**
   * Binds the given task to the given routing context.
   *
   * @param context The routing context.
   * @param task    The task to set.
   */
  public static void setTask(@NotNull RoutingContext context, @NotNull Task<?> task) {
    context.put(TASK, task);
    // This ensures that the start-nanos are set (we need to do this latest when binding the task).
    startNanos(context);
  }

  /**
   * Return the currently bound to the context.
   *
   * @param context The routing context.
   * @return The task; if any.
   */
  public static <E extends Event<E>> @Nullable Task<E> task(final @NotNull RoutingContext context) {
    Object raw = context.get(TASK);
    if (raw instanceof Task) {
      //noinspection unchecked
      return (Task<E>) raw;
    }
    return null;
  }

  /**
   * Returns the log-id, if possible from the current task; otherwise from the given caller.
   *
   * @param context the routing context.
   * @return the log-id.
   */
  public static @NotNull String logId(@NotNull RoutingContext context) {
    final Task<?> task = task(context);
    if (task != null) {
      return task.logId();
    }
    // TODO: If possible, query for the caller.
    return Context.class.getSimpleName();
  }

  /**
   * Returns the stream-id (either read from HTTP header or a generated one).
   *
   * @return The stream-id.
   */
  public static @NotNull String logStream(final @NotNull RoutingContext context) {
    Object raw = context.get(STREAM_ID);
    if (raw instanceof String) {
      return (String) raw;
    }
    String streamId = context.request().getHeader(STREAM_ID);
    if (streamId == null) {
      streamId = RandomStringUtils.randomAlphanumeric(12);
    }
    context.put(STREAM_ID, streamId);
    return streamId;
  }

  /**
   * Returns the creation time of the routing context in nanoseconds.
   *
   * @param context The context.
   * @return The creation time in nanoseconds.
   */
  public static @NotNull Long startNanos(final @NotNull RoutingContext context) {
    Object raw = context.get(START_NANOS);
    if (raw instanceof Long) {
      return (Long) raw;
    }
    final Long startNanos = NanoTime.now();
    context.put(START_NANOS, startNanos);
    return startNanos;
  }

  /**
   * Returns the current log time for the given routing context in microseconds.
   *
   * @param context the context.
   * @return The current log time for the given routing context in microseconds.
   */
  public static long logTime(final @NotNull RoutingContext context) {
    return NanoTime.timeSince(startNanos(context), TimeUnit.MICROSECONDS);
  }

  /**
   * Returns the access log object for this request.
   *
   * @param context the routing context.
   * @return the access log object.
   */
  public static @NotNull AccessLog accessLog(final @NotNull RoutingContext context) {
    AccessLog accessLog = context.get(ACCESS_LOG);
    if (accessLog == null) {
      accessLog = new AccessLog();
      context.put(ACCESS_LOG, accessLog);
    }
    return accessLog;
  }

  /**
   * Returns the JWT payload for the request.
   *
   * @return The JWT payload for the request.
   */
  public static @Nullable JWTPayload jwt(final @NotNull RoutingContext context) {
    JWTPayload payload = context.get(JWT);
    final User user;
    if (payload == null && (user = context.user()) != null) {
      payload = DatabindCodec.mapper().convertValue(user.principal(), JWTPayload.class);
      context.put(JWT, payload);
    }
    return payload;
  }

  /**
   * Returns the custom parsed query parameters.
   * <p>
   * Temporary solution until https://github.com/vert-x3/issues/issues/380 is resolved.
   */
  private static final String[] nonDecodeList = {Query.TAGS};

  /**
   * Returns the query parameters parsed by our self due to a bug in vertx, see: <a href="https://github.com/vert-x3/issues/issues/380>vertx
   * issue 380</a>
   *
   * @param context the routing context.
   * @return The parsed query parameters.
   */
  public static @NotNull MultiMap queryParameters(final @NotNull RoutingContext context) {
    MultiMap queryParams = context.get(QUERY_PARAMS);
    if (queryParams != null) {
      return queryParams;
    }
    final MultiMap map = MultiMap.caseInsensitiveMultiMap();
    String query = context.request().query();
    if (query != null && query.length() > 0) {
      String[] paramStrings = query.split("&");
      for (String paramString : paramStrings) {
        int eqDelimiter = paramString.indexOf("=");
        if (eqDelimiter > 0) {
          String key = paramString.substring(0, eqDelimiter);
          boolean decode = !ArrayUtils.contains(nonDecodeList, key);
          String rawValue = paramString.substring(eqDelimiter + 1);
          if (rawValue.length() > 0) {
            String[] values = rawValue.split(",");
            Stream.of(values).forEach(v -> {
              try {
                map.add(key, (decode ? URLDecoder.decode(v, Charset.defaultCharset().name()) : v));
              } catch (UnsupportedEncodingException ignored) {
              }
            });
          }
        }
      }
    }
    context.put(QUERY_PARAMS, map);
    return map;
  }
}
