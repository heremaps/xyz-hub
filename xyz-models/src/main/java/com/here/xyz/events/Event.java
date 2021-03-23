/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.Payload;
import java.util.HashMap;
import java.util.Map;

/**
 * The base class of all events that are send by the XYZ Hub to a "procedure". All events extend this event. All "procedures" can be sure to
 * receive events that extend this class and need to respond with any {@link com.here.xyz.responses.XyzResponse}.
 *
 * Its not defined if that procedure is embedded into the XYZ Hub or located at a remote host nor is any assumption being made about how the
 * event or response are transferred. Basically the event-response model just describes what events the XYZ hub may trigger and how the
 * processing "procedures" must respond.Ø
 *
 * A "procedure" is defined as
 *
 * Every event is basically encoded into a binary using an "procedure encoder". Be aware that this event is translated into some protocol
 * using a corresponding encoder. The protocol encoder  only the remote procedure client will receive this event. It's not necessary that
 * the remote procedure itself uses this event class to communicate. Rather the remote procedure client needs to accept the event, translate
 * it into an arbitrary binary (byte[]), which is then send to a remote service that processed the event.
 */
@JsonSubTypes({
    @JsonSubTypes.Type(value = ModifySpaceEvent.class, name = "ModifySpaceEvent"),
    @JsonSubTypes.Type(value = ModifyFeaturesEvent.class, name = "ModifyFeaturesEvent"),
    @JsonSubTypes.Type(value = TransformEvent.class, name = "TransformEvent"),
    @JsonSubTypes.Type(value = RelocatedEvent.class, name = "RelocatedEvent"),
    @JsonSubTypes.Type(value = EventNotification.class, name = "EventNotification"),
    @JsonSubTypes.Type(value = DeleteFeaturesByTagEvent.class, name = "DeleteFeaturesByTagEvent"),
    @JsonSubTypes.Type(value = SearchForFeaturesEvent.class, name = "SearchForFeaturesEvent"),
    @JsonSubTypes.Type(value = SearchForFeaturesOrderByEvent.class, name = "SearchForFeaturesOrderByEvent"),
    @JsonSubTypes.Type(value = IterateFeaturesEvent.class, name = "IterateFeaturesEvent"),
    @JsonSubTypes.Type(value = GetFeaturesByBBoxEvent.class, name = "GetFeaturesByBBoxEvent"),
    @JsonSubTypes.Type(value = GetFeaturesByGeometryEvent.class, name = "GetFeaturesByGeometryEvent"),
    @JsonSubTypes.Type(value = GetFeaturesByTileEvent.class, name = "GetFeaturesByTileEvent"),
    @JsonSubTypes.Type(value = CountFeaturesEvent.class, name = "CountFeaturesEvent"),
    @JsonSubTypes.Type(value = GetStatisticsEvent.class, name = "GetStatisticsEvent"),
    @JsonSubTypes.Type(value = GetHistoryStatisticsEvent.class, name = "GetHistoryStatisticsEvent"),
    @JsonSubTypes.Type(value = HealthCheckEvent.class, name = "HealthCheckEvent"),
    @JsonSubTypes.Type(value = GetFeaturesByIdEvent.class, name = "GetFeaturesByIdEvent"),
    @JsonSubTypes.Type(value = LoadFeaturesEvent.class, name = "LoadFeaturesEvent"),
    @JsonSubTypes.Type(value = IterateHistoryEvent.class, name = "IterateHistoryEvent"),
    @JsonSubTypes.Type(value = ContentModifiedNotification.class, name = "ContentModifiedNotification")
})

public abstract class Event<T extends Event> extends Payload {

  public static final String VERSION = "0.4.0";

  @JsonView(ExcludeFromHash.class)
  private Map<String, Object> connectorParams;
  @JsonView(ExcludeFromHash.class)
  private String streamId;
  @JsonView(ExcludeFromHash.class)
  private String ifNoneMatch;
  @JsonView(ExcludeFromHash.class)
  private Boolean preferPrimaryDataSource;
  @JsonView(ExcludeFromHash.class)
  private Map<String, Object> params;
  private TrustedParams trustedParams;
  private String space;
  private Map<String, Object> metadata;
  @JsonView(ExcludeFromHash.class)
  private String tid;
  @JsonView(ExcludeFromHash.class)
  private String jwt;
  @JsonView(ExcludeFromHash.class)
  private String aid;
  @JsonView(ExcludeFromHash.class)
  private String version = VERSION;

  /**
   * The identifier of the space.
   *
   * @return the identifier of the space.
   */
  public String getSpace() {
    return this.space;
  }

  @SuppressWarnings("unused")
  public void setSpace(String space) {
    this.space = space;
  }

  @SuppressWarnings("unused")
  public T withSpace(String space) {
    setSpace(space);
    //noinspection unchecked
    return (T) this;
  }

  /**
   * An map with arbitrary parameters configured in the XYZ Hub service for each space. Therefore, each space can have different
   * parameters.
   *
   * @return a map with arbitrary parameters defined for the space.
   */
  public Map<String, Object> getParams() {
    return this.params;
  }

  @SuppressWarnings("WeakerAccess")
  public void setParams(Map<String, Object> params) {
    this.params = params;
  }

  @SuppressWarnings("unused")
  public T withParams(Map<String, Object> params) {
    setParams(params);
    //noinspection unchecked
    return (T) this;
  }

  /**
   * A parameter map which may contains sensitive information such as identities and is forwarded only to connectors
   * marked with "trusted" flag.
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

  @SuppressWarnings("unused")
  public T withTrustedParams(TrustedParams trustedParams) {
    setTrustedParams(trustedParams);
    //noinspection unchecked
    return (T) this;
  }

  /**
   * The stream identifier that should be used for logging purpose. In fact the XYZ Hub service will internally generate a unique stream
   * identifier for every request it receives and log everything that happens while processing this request using this stream identifier. Be
   * aware that there can be multiple stream using the same connection, for example in HTTP 2 or WebSockets.
   *
   * @return the stream identifier.
   */
  public String getStreamId() {
    return this.streamId;
  }

  @SuppressWarnings("UnusedReturnValue")
  public void setStreamId(String streamId) {
    this.streamId = streamId;
  }

  public T withStreamId(String streamId) {
    this.streamId = streamId;
    //noinspection unchecked
    return (T) this;
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

  @SuppressWarnings("unused")
  public T withIfNoneMatch(String ifNoneMatch) {
    setIfNoneMatch(ifNoneMatch);
    //noinspection unchecked
    return (T) this;
  }

  /**
   * The connector parameters.
   */
  public Map<String, Object> getConnectorParams() {
    return this.connectorParams;
  }

  @SuppressWarnings("WeakerAccess")
  public void setConnectorParams(Map<String, Object> connectorParams) {
    this.connectorParams = connectorParams;
  }

  @SuppressWarnings("unused")
  public T withConnectorParams(Map<String, Object> connectorParams) {
    setConnectorParams(connectorParams);
    //noinspection unchecked
    return (T) this;
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

  @SuppressWarnings("unused")
  public T withMetadata(Map<String, Object> metadata) {
    setMetadata(metadata);
    //noinspection unchecked
    return (T) this;
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

  @SuppressWarnings("unused")
  public T withTid(String tid) {
    setTid(tid);
    //noinspection unchecked
    return (T) this;
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

  @SuppressWarnings("unused")
  public T withJwt(String jwt) {
    setJwt(jwt);
    //noinspection unchecked
    return (T) this;
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

  @SuppressWarnings("unused")
  public T withAid(String aid) {
    setAid(aid);
    //noinspection unchecked
    return (T) this;
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

  @SuppressWarnings("unused")
  public T withPreferPrimaryDataSource(Boolean preferPrimaryDataSource) {
    setPreferPrimaryDataSource(preferPrimaryDataSource);
    //noinspection unchecked
    return (T) this;
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

  @SuppressWarnings("unused")
  public T withVersion(String version) {
    setVersion(version);
    //noinspection unchecked
    return (T) this;
  }

  @Override
  public String toString() {
    return serialize();
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
      if (cookies == null) return;
      put(COOKIES, cookies);
    }

    public void putCookie(String name, String value) {
      if (!containsKey(COOKIES)) put(COOKIES, new HashMap<String, String>());
      getCookies().put(name, value);
    }

    public String getCookie(String name) {
      if (containsKey(COOKIES)) return getCookies().get(name);
      return null;
    }

    public Map<String, String> getHeaders() {
      //noinspection unchecked
      return (Map<String, String>) get(HEADERS);
    }

    public void setHeaders(Map<String, String> headers) {
      if (headers == null) return;
      put(HEADERS, headers);
    }

    public void putHeader(String name, String value) {
      if (!containsKey(HEADERS)) put(HEADERS, new HashMap<String, String>());
      getHeaders().put(name, value);
    }

    public String getHeader(String name) {
      if (containsKey(HEADERS)) return getHeaders().get(name);
      return null;
    }

    public Map<String, String> getQueryParams() {
      //noinspection unchecked
      return (Map<String, String>) get(QUERY_PARAMS);
    }

    public void setQueryParams(Map<String, String> queryParams) {
      if (queryParams == null) return;
      put(QUERY_PARAMS, queryParams);
    }

    public void putQueryParam(String name, String value) {
      if (!containsKey(QUERY_PARAMS)) put(QUERY_PARAMS, new HashMap<String, String>());
      getQueryParams().put(name, value);
    }

    public String getQueryParam(String name) {
      if (containsKey(QUERY_PARAMS)) return getQueryParams().get(name);
      return null;
    }
  }
}
