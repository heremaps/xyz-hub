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

package com.here.xyz.events;

import static com.here.xyz.EventTask.currentTask;
import static com.here.xyz.EventTask.currentTaskOrNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.NanoTime;
import com.here.xyz.Payload;
import com.here.xyz.events.admin.ModifySubscriptionEvent;
import com.here.xyz.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.events.feature.IterateFeaturesEvent;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.feature.history.IterateHistoryEvent;
import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.events.info.GetHistoryStatisticsEvent;
import com.here.xyz.events.info.GetStatisticsEvent;
import com.here.xyz.events.info.GetStorageStatisticsEvent;
import com.here.xyz.events.info.HealthCheckEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.EventTask;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The base class of all events that are sent by the XYZ Hub to a "procedure". All events extend this event. All "procedures" can be sure to
 * receive events that extend this class and need to respond with any {@link com.here.xyz.responses.XyzResponse}.
 *
 * <p>It's not defined if that procedure is embedded into the XYZ Hub or located at a remote host
 * nor is any assumption being made about how the event or response are transferred. Basically the event-response model just describes what
 * events the XYZ hub may trigger and how the processing "procedures" must respond.
 *
 * <p>A "procedure" is defined as
 *
 * <p>Every event is basically encoded into a binary using a "procedure encoder". Be aware that this
 * event is translated into some protocol using a corresponding encoder. Only the remote procedure client will receive this event. It's not
 * necessary that the remote procedure itself uses this event class to communicate. Rather the remote procedure client needs to accept the
 * event, translate it into an arbitrary binary (byte[]), which is then sent to a remote service that processes the event.
 */
@SuppressWarnings("unused")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ModifySpaceEvent.class, name = "ModifySpaceEvent"),
    @JsonSubTypes.Type(value = ModifySubscriptionEvent.class, name = "ModifySubscriptionEvent"),
    @JsonSubTypes.Type(value = ModifyFeaturesEvent.class, name = "ModifyFeaturesEvent"),
    @JsonSubTypes.Type(value = DeleteFeaturesByTagEvent.class, name = "DeleteFeaturesByTagEvent"),
    @JsonSubTypes.Type(value = SearchForFeaturesEvent.class, name = "SearchForFeaturesEvent"),
    @JsonSubTypes.Type(value = IterateFeaturesEvent.class, name = "IterateFeaturesEvent"),
    @JsonSubTypes.Type(value = GetFeaturesByBBoxEvent.class, name = "GetFeaturesByBBoxEvent"),
    @JsonSubTypes.Type(value = GetFeaturesByGeometryEvent.class, name = "GetFeaturesByGeometryEvent"),
    @JsonSubTypes.Type(value = GetFeaturesByTileEvent.class, name = "GetFeaturesByTileEvent"),
    @JsonSubTypes.Type(value = GetStatisticsEvent.class, name = "GetStatisticsEvent"),
    @JsonSubTypes.Type(value = GetStorageStatisticsEvent.class, name = "GetStorageStatisticsEvent"),
    @JsonSubTypes.Type(value = GetHistoryStatisticsEvent.class, name = "GetHistoryStatisticsEvent"),
    @JsonSubTypes.Type(value = HealthCheckEvent.class, name = "HealthCheckEvent"),
    @JsonSubTypes.Type(value = GetFeaturesByIdEvent.class, name = "GetFeaturesByIdEvent"),
    @JsonSubTypes.Type(value = LoadFeaturesEvent.class, name = "LoadFeaturesEvent"),
    @JsonSubTypes.Type(value = IterateHistoryEvent.class, name = "IterateHistoryEvent")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Event extends Payload {

  protected Event() {
    final EventTask context = currentTaskOrNull();
    if (context != null) {
      startNanos = context.startNanos;
      streamId = context.streamId();
    } else {
      startNanos = NanoTime.now();
    }
  }

  /**
   * The time when the event started processed.
   */
  @JsonIgnore
  private long startNanos;

  @JsonIgnore
  public long startNanos() {
    return startNanos;
  }

  @JsonIgnore
  public void setStartNanos(long nanos) {
    this.startNanos = nanos;
  }

  /**
   * Returns the microseconds passed since the start of the event processing.
   *
   * @return the microseconds passed since the start of the event processing.
   */
  @JsonIgnore
  public final long micros() {
    return NanoTime.timeSince(startNanos, MICROSECONDS);
  }

  /**
   * Returns the milliseconds passed since the start of the event processing.
   *
   * @return the milliseconds passed since the start of the event processing.
   */
  @JsonIgnore
  public final long millis() {
    return NanoTime.timeSince(startNanos, TimeUnit.MILLISECONDS);
  }

  private static final long DEFAULT_TTL_MICROS = TimeUnit.MINUTES.toMicros(1);

  /**
   * Returns the amount of time remaining for this event to be processed.
   *
   * @param timeUnit the time-unit in which to return the remaining time.
   * @return the remaining time, minimally 0.
   */
  @JsonIgnore
  public long remaining(@NotNull TimeUnit timeUnit) {
    final long remaining = MICROSECONDS.convert(Math.max(DEFAULT_TTL_MICROS - micros(), 0), timeUnit);
    assert remaining >= 0L;
    return remaining;
  }

  @JsonView(ExcludeFromHash.class)
  private @Nullable Map<@NotNull String, @Nullable Object> connectorParams;

  @JsonView(ExcludeFromHash.class)
  private String streamId;

  @JsonView(ExcludeFromHash.class)
  private String ifNoneMatch;

  @JsonView(ExcludeFromHash.class)
  private Boolean preferPrimaryDataSource;

  @JsonView(ExcludeFromHash.class)
  private Map<String, Object> params;

  private TrustedParams trustedParams;

  /**
   * The unique space identifier.
   */
  @JsonProperty
  private String space;

  private Map<String, Object> metadata;

  @JsonView(ExcludeFromHash.class)
  private String tid;

  @JsonView(ExcludeFromHash.class)
  private String jwt;

  /**
   * The application-id of the application sending the event.
   */
  @JsonView(ExcludeFromHash.class)
  private @Nullable String aid;

  @JsonView(ExcludeFromHash.class)
  @JsonInclude(Include.ALWAYS)
  private String version = VERSION;

  @JsonProperty
  private @Nullable String author;

  /**
   * The identifier of the space.
   *
   * @return the identifier of the space.
   */
  @JsonIgnore
  public @Nullable String getSpace() {
    return this.space;
  }

  @JsonIgnore
  public void setSpace(@NotNull String space) {
    this.space = space;
  }

  /**
   * The space parameter from {@link Space#params}.
   */
  public @Nullable Map<@NotNull String, Object> getParams() {
    return this.params;
  }

  public void setParams(@Nullable Map<@NotNull String, Object> params) {
    this.params = params;
  }

  /**
   * A parameter map which may contains sensitive information such as identities and is forwarded only to connectors marked with "trusted"
   * flag.
   *
   * @return a map with arbitrary parameters.
   */
  public TrustedParams getTrustedParams() {
    return this.trustedParams;
  }

  @SuppressWarnings("WeakerAccess")
  public void setTrustedParams(TrustedParams trustedParams) {
    this.trustedParams = trustedParams;
  }

  /**
   * The stream identifier that should be used for logging purpose. In fact the XYZ Hub service will internally generate a unique stream
   * identifier for every request it receives and log everything that happens while processing this request using this stream identifier. Be
   * aware that there can be multiple stream using the same connection, for example in HTTP 2 or WebSockets.
   *
   * @return the stream identifier.
   */
  @JsonIgnore
  public @NotNull String getStreamId() {
    if (streamId == null) {
      final EventTask context = currentTaskOrNull();
      if (context != null) {
        streamId = context.streamId();
      } else {
        streamId = RandomStringUtils.randomAlphanumeric(12);
      }
    }
    return streamId;
  }

  @JsonIgnore
  @SuppressWarnings("UnusedReturnValue")
  public void setStreamId(@NotNull String streamId) {
    this.streamId = streamId;
  }

  /**
   * If the client has provided an E-Tag for this event this value is forwarded. This can be used to cache responses.
   *
   * @return the e-tag, if any.
   */
  public String getIfNoneMatch() {
    return this.ifNoneMatch;
  }

  @SuppressWarnings("WeakerAccess")
  public void setIfNoneMatch(String ifNoneMatch) {
    this.ifNoneMatch = ifNoneMatch;
  }

  /**
   * The parameters as {@link Connector#params configured in the connector}.
   */
  public @Nullable Map<@NotNull String, @Nullable Object> getConnectorParams() {
    return this.connectorParams;
  }

  /**
   * The parameters as {@link Connector#params configured in the connector}.
   */
  public @NotNull Map<@NotNull String, @Nullable Object> withConnectorParams() {
    if (connectorParams == null) {
      connectorParams = new HashMap<>();
    }
    return connectorParams;
  }

  @SuppressWarnings("WeakerAccess")
  public void setConnectorParams(@Nullable Map<@NotNull String, @Nullable Object> connectorParams) {
    this.connectorParams = connectorParams;
  }

  // TODO: Move connectorId and connectorNumber to root!
  // Note: connectorParams are optional and only necessary when sending the event to a remove location.
  //       This means, they are basically Virtual

  /**
   * Returns the "connectorId" property from the {@link #getConnectorParams() connector parameters}.
   *
   * @return the "connectorId" property; if set.
   */
  public @NotNull String getConnectorId() {
    final Map<@NotNull String, @Nullable Object> params = getConnectorParams();
    if (params != null) {
      final Object raw = params.get("connectorId");
      if (raw instanceof String) {
        return (String) raw;
      }
    }
    currentTask().debug("Missing 'connectorParams.connectorId' in event");
    return getClass().getName();
  }

  /**
   * Sets the "connectorId" property in the {@link #getConnectorParams() connector parameters}. If no params exists yet, new params are
   * created, and the connector-id is set in it.
   *
   * @param id the connector ID to be set.
   */
  public void setConnectorId(@NotNull String id) {
    withConnectorParams().put("connectorId", id);
  }

  /**
   * The identifier of the space.
   *
   * @return the identifier of the space.
   */
  @JsonIgnore
  public long getConnectorNumber() {
    final Map<@NotNull String, @Nullable Object> params = getConnectorParams();
    if (params != null) {
      final Object raw = params.get("connectorNumber");
      if (raw instanceof Number) {
        return ((Number) raw).longValue();
      }
    }
    currentTask().debug("Missing 'connectorParams.connectorNumber' in event");
    return 0L;
  }

  @JsonIgnore
  public void setConnectorNumber(long connectorNumber) {
    withConnectorParams().put("connectorNumber", connectorNumber);
  }

  /**
   * The user-defined metadata coming from the user's token being used for the request. The serialized metadata is guaranteed to be not
   * larger than 256 bytes.
   *
   * @return The metadata or null if there is none.
   */
  public Map<String, Object> getMetadata() {
    return this.metadata;
  }

  @SuppressWarnings("unused")
  public void setMetadata(Map<String, Object> metadata) {
    this.metadata = metadata;
  }

  /**
   * The token ID of the token being used for the request. NOTE: This field will only be sent to "trusted" connectors.
   */
  public String getTid() {
    return this.tid;
  }

  @SuppressWarnings("WeakerAccess")
  public void setTid(String tid) {
    this.tid = tid;
  }

  /**
   * The complete JWT token to be forwarded to trusted connectors.
   */
  public String getJwt() {
    return this.jwt;
  }

  @SuppressWarnings("WeakerAccess")
  public void setJwt(String jwt) {
    this.jwt = jwt;
  }

  /**
   * The users account ID of the token being used for the request. NOTE: This field will only be sent to "trusted" connectors.
   */
  @SuppressWarnings("unused")
  public String getAid() {
    return this.aid;
  }

  @SuppressWarnings({"unused", "WeakerAccess"})
  public void setAid(String aid) {
    this.aid = aid;
  }

  /**
   * The users account ID of the token being used for the request. NOTE: This field will only be sent to "trusted" connectors.
   */
  public @Nullable String getAuthor() {
    return this.author;
  }

  public void setAuthor(@Nullable String author) {
    this.author = author;
  }

  /**
   * A boolean parameter, which instructs the connector, that when possible, the primary data source is preferred, in case the connector
   * uses both primary and replica data sources.
   *
   * @return if the primary data source is preferred
   */
  @SuppressWarnings("unused")
  public Boolean getPreferPrimaryDataSource() {
    return this.preferPrimaryDataSource;
  }

  @SuppressWarnings("WeakerAccess")
  public void setPreferPrimaryDataSource(Boolean preferPrimaryDataSource) {
    this.preferPrimaryDataSource = preferPrimaryDataSource;
  }

  /**
   * The version of the event protocol.
   *
   * @return The version
   */
  public String getVersion() {
    return this.version;
  }

  @SuppressWarnings("unused")
  public void setVersion(String version) {
    this.version = version;
  }

  public static class TrustedParams extends HashMap<String, Object> {

    public static final String COOKIES = "cookies";
    public static final String HEADERS = "headers";
    public static final String QUERY_PARAMS = "queryParams";

    public Map<String, String> getCookies() {
      //noinspection unchecked
      return (Map<String, String>) get(COOKIES);
    }

    public void setCookies(Map<String, String> cookies) {
      if (cookies == null) {
        return;
      }
      put(COOKIES, cookies);
    }

    public void putCookie(String name, String value) {
      if (!containsKey(COOKIES)) {
        put(COOKIES, new HashMap<String, String>());
      }
      getCookies().put(name, value);
    }

    public String getCookie(String name) {
      if (containsKey(COOKIES)) {
        return getCookies().get(name);
      }
      return null;
    }

    public Map<String, String> getHeaders() {
      //noinspection unchecked
      return (Map<String, String>) get(HEADERS);
    }

    public void setHeaders(Map<String, String> headers) {
      if (headers == null) {
        return;
      }
      put(HEADERS, headers);
    }

    public void putHeader(String name, String value) {
      if (!containsKey(HEADERS)) {
        put(HEADERS, new HashMap<String, String>());
      }
      getHeaders().put(name, value);
    }

    public String getHeader(String name) {
      if (containsKey(HEADERS)) {
        return getHeaders().get(name);
      }
      return null;
    }

    public Map<String, String> getQueryParams() {
      //noinspection unchecked
      return (Map<String, String>) get(QUERY_PARAMS);
    }

    public void setQueryParams(Map<String, String> queryParams) {
      if (queryParams == null) {
        return;
      }
      put(QUERY_PARAMS, queryParams);
    }

    public void putQueryParam(String name, String value) {
      if (!containsKey(QUERY_PARAMS)) {
        put(QUERY_PARAMS, new HashMap<String, String>());
      }
      getQueryParams().put(name, value);
    }

    public String getQueryParam(String name) {
      if (containsKey(QUERY_PARAMS)) {
        return getQueryParams().get(name);
      }
      return null;
    }
  }
}
