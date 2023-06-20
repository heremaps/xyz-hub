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

package com.here.xyz.models.payload;

import static com.here.xyz.AbstractTask.currentTask;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.EventPipeline;
import com.here.xyz.IEventContext;
import com.here.xyz.IEventHandler;
import com.here.xyz.INaksha;
import com.here.xyz.util.NanoTime;
import com.here.xyz.models.Payload;
import com.here.xyz.models.payload.events.admin.ModifySubscriptionEvent;
import com.here.xyz.models.payload.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.models.payload.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.models.payload.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.models.payload.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.models.payload.events.feature.IterateFeaturesEvent;
import com.here.xyz.models.payload.events.feature.LoadFeaturesEvent;
import com.here.xyz.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.xyz.models.payload.events.feature.history.IterateHistoryEvent;
import com.here.xyz.models.payload.events.feature.SearchForFeaturesEvent;
import com.here.xyz.models.payload.events.info.GetHistoryStatisticsEvent;
import com.here.xyz.models.payload.events.info.GetStatisticsEvent;
import com.here.xyz.models.payload.events.info.GetStorageStatisticsEvent;
import com.here.xyz.models.payload.events.info.HealthCheckEvent;
import com.here.xyz.models.payload.events.space.ModifySpaceEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.models.hub.pipelines.Space;
import com.here.xyz.AbstractTask;
import com.here.xyz.util.json.JsonSerializable;
import com.here.xyz.util.json.JsonUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The base class of all events that are sent by the Hub to a {@link IEventHandler handler}. All events extend this event. All
 * {@link IEventHandler handler} can be sure to receive events that extend this class and need to respond with an object extending the
 * {@link XyzResponse} class.
 *
 * <p>An event can be serialized and send to another instance to be processed there, this is decided by the {@link EventPipeline}. When
 * a handler sends an event {@link IEventContext#sendUpstream() upstream}, the handler itself does not know if this event is processed in
 * the current host or on a foreign host. Basically the event model just describes what events exist and what the event should produce.
 *
 * <p>Every event can be encoded/decoded into/from any binary form using an encoder/decoder. It is unnecessary, that the remote procedure
 * itself uses this class or any code from this package to handle the event.
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
public class Event extends Payload {

  protected Event() {
    startNanos = NanoTime.now();
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
  private String streamId;

  @JsonView(ExcludeFromHash.class)
  private String ifNoneMatch;

  @JsonView(ExcludeFromHash.class)
  private Boolean preferPrimaryDataSource;

  @JsonView(ExcludeFromHash.class)
  private @Nullable Map<@NotNull String, @Nullable Object> params;

  /**
   * The unique space identifier.
   */
  @JsonProperty("space")
  private @Nullable String spaceId;

  /**
   * The internal cache of the space.
   */
  @JsonIgnore
  private @Nullable Space space;

  /**
   * The collection; if any.
   */
  @JsonProperty
  private @Nullable String collection;

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
  private String version = INaksha.v2_0;

  @JsonProperty
  private @Nullable String author;

  /**
   * The identifier of the collection; if any.
   *
   * @return the identifier of the collection; if any.
   */
  @JsonIgnore
  public @Nullable String getCollection() {
    return this.collection;
  }

  /**
   * The identifier of the collection or the alternative.
   *
   * @param alternative The alternative to return, if no collection is available.
   * @return The identifier of the collection or the alternative, if no collection is available.
   */
  @JsonIgnore
  public @NotNull String getCollection(@NotNull String alternative) {
    return collection != null ? collection : alternative;
  }

  /**
   * The identifier of the space.
   *
   * @return the identifier of the space.
   */
  @JsonIgnore
  public @Nullable String getSpaceId() {
    return this.spaceId;
  }

  /**
   * Sets the {@link #spaceId} and {@link #collection}.
   *
   * @param space The space to set.
   */
  @JsonIgnore
  public void setSpace(@NotNull Space space) {
    this.spaceId = space.getId();
    this.collection = space.getCollection();
    this.params = JsonSerializable.deepClone(space.getProperties());
    this.space = space;
  }

  /**
   * Returns the space; if the referred space exists.
   *
   * @return The space; if the referred space exists.
   * @throws ParameterError If no such space exists (so space-id given, but no such space exists).
   */
  public @NotNull Space getSpace() throws ParameterError {
    if (space == null && spaceId != null) {
      space = INaksha.get().getSpaceById(spaceId);
    }
    if (space == null) {
      throw new ParameterError("Missing or invalid spaceId: " + spaceId);
    }
    return space;
  }

  /**
   * The space parameter from {@link Space#getProperties() properties}.
   */
  public @NotNull Map<@NotNull String, @Nullable Object> getParams() {
    if (params == null) {
      params = new HashMap<>();
    }
    return params;
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
      final AbstractTask<?> task = currentTask();
      if (task != null) {
        streamId = task.streamId();
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
  public @Nullable String getAid() {
    return this.aid;
  }

  @SuppressWarnings({"unused", "WeakerAccess"})
  public void setAid(@Nullable String aid) {
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

}
