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

package com.here.xyz.hub.connectors.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.primitives.Longs;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * The space configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Space")
@JsonInclude(Include.NON_DEFAULT)
public class Space extends com.here.xyz.models.hub.Space implements Cloneable {

  private static final Logger logger = LogManager.getLogger();
  private static final long DEFAULT_CONTENT_UPDATED_AT_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
  private static final long NO_CACHE_INTERVAL_MILLIS = DEFAULT_CONTENT_UPDATED_AT_INTERVAL_MILLIS * 2;
  private static final long MIN_SERVICE_CACHE_INTERVAL_MILLIS = TimeUnit.DAYS.toMillis(1);

  /**
   * Add random 20 seconds offset to avoid all service nodes sending cache invalidation for the space at the same time
   */
  public static final long CONTENT_UPDATED_AT_INTERVAL_MILLIS = DEFAULT_CONTENT_UPDATED_AT_INTERVAL_MILLIS
      - TimeUnit.SECONDS.toMillis((long) (Math.random() * 20));

  private final static long MAX_SLIDING_WINDOW = TimeUnit.DAYS.toMillis(10);
  /**
   * Indicates the last time the content of a space was updated.
   */
  @JsonInclude(Include.NON_DEFAULT)
  @JsonView({Public.class, Static.class})
  public long contentUpdatedAt = 0;

  @JsonInclude(Include.NON_DEFAULT)
  @JsonView({Public.class, Static.class})
  public boolean notSendDeleteMse = false;

  /**
   * An indicator, if the data in the space is edited often (value tends to 1) or static (value tends to 0).
   */
  @JsonView({Internal.class, Static.class})
  public double volatilityAtLastContentUpdate = 0;

  @JsonIgnore
  private Map<ConnectorType, Map<String, List<ResolvableListenerConnectorRef>>> resolvedConnectorRefs;

  private String region;

  @Deprecated
  //TODO: Remove this once Job-API was fixed to configure that on job-level
  private boolean persistExport;

  public static Future<Space> resolveSpace(Marker marker, String spaceId) {
    return Service.spaceConfigClient.get(marker, spaceId);
  }

  public static Future<Connector> resolveConnector(Marker marker, String connectorId) {
    Promise<Connector> p = Promise.promise();
    resolveConnector(marker, connectorId, p);
    return p.future();
  }

  public static void resolveConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    Service.connectorConfigClient.get(marker, connectorId, arStorage -> {
      if (arStorage.failed()) {
        logger.warn(marker, "Unable to load the connector definition for storage '{}'",
            connectorId, arStorage.cause());
      } else {
        final Connector storage = arStorage.result();
        logger.info(marker, "Loaded storage, configuration is: {}", io.vertx.core.json.Json.encode(storage));
      }
      handler.handle(arStorage);
    });
  }

  public Future<Map<String, Object>> resolveCompositeParams(Marker marker) {
    if (getExtension() == null)
      return Future.succeededFuture(Collections.emptyMap());

    return resolveSpace(marker, getExtension().getSpaceId())
        .flatMap(extendedSpace -> extendedSpace == null ?
                Future.failedFuture("Unable to load extended resource with id: " + getExtension().getSpaceId()) :
                Future.succeededFuture(resolveCompositeParams(extendedSpace)));
  }

  public Map<String, Object> resolveCompositeParams(Space extendedSpace) {
    if (getExtension() == null)
      return Collections.emptyMap();
    //Storage params are taken from the input and then resolved based on the extensions
    final Map<String, Object> extendsMap = getExtension().asMap();

    //TODO: Remove this once Job-API was fixed to configure that on job-level
    if (extendedSpace.isPersistExport())
      extendsMap.put("persistExport", true);

    //Check if the extended space itself is extending some other space (2-level extension)
    if (extendedSpace != null && extendedSpace.getExtension() != null)
      //Get the extension definition from the extended space and add it to this one additionally
      extendsMap.put("extends", extendedSpace.getExtension().asMap());
    return Collections.singletonMap("extends", extendsMap);
  }

  @JsonView(Internal.class)
  @SuppressWarnings("unused")
  public CacheProfile getAutoCacheProfile() {
    return getCacheProfile(false, true, false);
  }

  @JsonView(Internal.class)
  public double getVolatility() {
    long now = Core.currentTimeMillis();

    // if the space existed for a shorter period of time, use this as a sliding window.
    long slidingWindow = Math.min(1 + now - getCreatedAt(), MAX_SLIDING_WINDOW);
    double averageInterval = slidingWindow * (1 - volatilityAtLastContentUpdate);

    // limit the interval to the length of the sliding window
    long interval = Math.min(now - getContentUpdatedAt(), slidingWindow);

    // update the average interval value
    averageInterval = (interval * interval + (slidingWindow - interval) * averageInterval) / slidingWindow;
    return (1 - averageInterval / slidingWindow);
  }

  @JsonIgnore
  public Map<String, List<ResolvableListenerConnectorRef>> getEventTypeConnectorRefsMap(ConnectorType connectorType) {
    if (resolvedConnectorRefs == null) {
      resolvedConnectorRefs = new ConcurrentHashMap<>();
    }
    resolvedConnectorRefs.computeIfAbsent(connectorType, k -> {
      List<Space.ListenerConnectorRef> connectorRefs = getConnectorRefs(connectorType);

      //"Explode" the resolved connector list into a map (by event-type) and remember it for later
      final Map<String, List<ResolvableListenerConnectorRef>> organizedRefs = new HashMap<>();
      connectorRefs.forEach(c -> {
        if (c.getEventTypes() != null) {
          c.getEventTypes().forEach(et -> {
            organizedRefs.computeIfAbsent(et, dummy -> new ArrayList<>());
            // hopefully this works
            organizedRefs.get(et).add((ResolvableListenerConnectorRef) c);
          });
        }
      });
      return organizedRefs;
    });
    return resolvedConnectorRefs.get(connectorType);
  }

  @JsonIgnore
  public Map<String, List<Space.ListenerConnectorRef>> getConnectorRefsMap(final ConnectorType connectorType) {
    if (connectorType == ConnectorType.LISTENER) {
      if (getListeners() == null) {
        return Collections.emptyMap();
      }
      return getListeners();
    } else if (connectorType == ConnectorType.PROCESSOR) {
      if (getProcessors() == null) {
        return Collections.emptyMap();
      }
      return getProcessors();
    } else {
      throw new RuntimeException("Unknown connector type"); //Will never happen if nobody extends the ConnectorType enum :)
    }
  }

  @JsonIgnore
  private List<Space.ListenerConnectorRef> getConnectorRefs(final ConnectorType connectorType) {
    return getConnectorRefsMap(connectorType)
        .values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public boolean hasRequestListeners() {
    if (getListeners() == null || getListeners().isEmpty()) return false;
    List<Space.ListenerConnectorRef> listeners = getConnectorRefs(ConnectorType.LISTENER);
    return listeners.stream().anyMatch(l -> l.getEventTypes() != null &&
        l.getEventTypes().stream().anyMatch(et -> et.endsWith(".request")));
  }

  @JsonIgnore
  public CacheProfile getCacheProfile(boolean skipCache, boolean autoConfig, boolean readOnlyAccess) {
    //Cache is manually deactivated by the user, either for the space or for this specific request
    if (getCacheTTL() == 0 || skipCache) {
      return CacheProfile.NO_CACHE;
    }

    //Cache is manually / user defined at the space -> use those settings instead
    if (getCacheTTL() > 0) {
      return new CacheProfile(getCacheTTL() / 3, getCacheTTL(), Long.MAX_VALUE, getCacheTTL(), getContentUpdatedAt());
    }

    //Automatic cache configuration is not supported at all
    if (!autoConfig) {
      return CacheProfile.NO_CACHE;
    }

    double volatility = getVolatility();
    long timeSinceLastUpdate = Core.currentTimeMillis() - getContentUpdatedAt();
    long staticTTL = readOnlyAccess ? CacheProfile.MAX_STATIC_TTL : 0;

    //For mutable responses and a space which was changed within the no-cache interval -> no cache
    if (!readOnlyAccess && timeSinceLastUpdate < NO_CACHE_INTERVAL_MILLIS) {
      return CacheProfile.NO_CACHE;
    }

    //For all other responses and a space which was changed
    //within the service-cache interval (minimum service-cache interval + some volatility penalty time) -> cache only in the service cache.
    //Also, if the response is mutable (not resulting from a read-only request) -> cache only in the service cache.
    long volatilityPenalty = (long) (volatility * volatility * TimeUnit.DAYS.toMillis(7));
    long serviceCacheInterval = MIN_SERVICE_CACHE_INTERVAL_MILLIS + volatilityPenalty;
    if (!readOnlyAccess || timeSinceLastUpdate < serviceCacheInterval)
      return new CacheProfile(0, 0, CacheProfile.MAX_SERVICE_TTL, staticTTL, getContentUpdatedAt());

    //For all other responses of a space which was not changed for longer time -> cache in the service *and* in the browser / CDN
    return new CacheProfile(TimeUnit.MINUTES.toMillis(3), TimeUnit.HOURS.toMillis(24), CacheProfile.MAX_SERVICE_TTL, staticTTL,
        getContentUpdatedAt());
  }

  public long getContentUpdatedAt() {
    if (contentUpdatedAt == 0) {
      contentUpdatedAt = getCreatedAt();
    }
    return contentUpdatedAt;
  }

  public void setContentUpdatedAt(long contentUpdatedAt) {
    this.contentUpdatedAt = contentUpdatedAt;
  }

  public Space withContentUpdatedAt(long contentUpdatedAt) {
    setContentUpdatedAt(contentUpdatedAt);
    return this;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public Space withRegion(String region) {
    setRegion(region);
    return this;
  }

  @Deprecated
  //TODO: Remove this once Job-API was fixed to configure that on job-level
  public boolean isPersistExport() {
    return persistExport;
  }

  @Deprecated
  //TODO: Remove this once Job-API was fixed to configure that on job-level
  public void setPersistExport(boolean persistExport) {
    this.persistExport = persistExport;
  }

  @Deprecated
  //TODO: Remove this once Job-API was fixed to configure that on job-level
  public Space withPersistExport(boolean persistExport) {
    setPersistExport(persistExport);
    return this;
  }

  public enum ConnectorType {
    LISTENER, PROCESSOR
  }

  /**
   * Extended space class, which includes the granted access rights.
   */
  public static class SpaceWithRights extends Space {

    @JsonView(Public.class)
    public List<String> rights;
  }

  // Dirty Hack: Suppress output of IDs when serializing ResolvableListenerConnectorRefs
  // This is only needed when we return connectors as Map and not as List
  @JsonIgnoreProperties(value = {"id"}, ignoreUnknown = true)
  public static class ResolvableListenerConnectorRef extends com.here.xyz.models.hub.Space.ListenerConnectorRef {

    @JsonIgnore
    public Connector resolvedConnector;
  }

  public static class CacheProfile {
    public static final CacheProfile NO_CACHE = new CacheProfile(0, 0, 0, 0, 0);
    private static final long MAX_BROWSER_TTL = TimeUnit.MINUTES.toMillis(3);
    private static final long MAX_CDN_TTL = TimeUnit.DAYS.toMillis(365);
    private static final long MAX_SERVICE_TTL = TimeUnit.DAYS.toMillis(365);
    private static final long MAX_STATIC_TTL = TimeUnit.DAYS.toMillis(365);

    /**
     * How long to cache the response in the browser cache in milliseconds.
     */
    public final long browserTTL;

    /**
     * How long to cache the response in a CDN cache in milliseconds.
     */
    public final long cdnTTL;

    /**
     * How long to cache the response in the volatile service cache in milliseconds.
     */
    @JsonIgnore
    public final long serviceTTL;

    /**
     * How long to cache the response in the static / persistent service cache in milliseconds.
     */
    public final long staticTTL;
    @JsonIgnore
    public final long contentUpdatedAt;

    @SuppressWarnings("UnstableApiUsage")
    public CacheProfile(long browserTTL, long cdnTTL, long serviceTTL, long staticTTL, long contentUpdatedAt) {
      this.browserTTL = Longs.constrainToRange(browserTTL, 0, MAX_BROWSER_TTL);
      this.cdnTTL = Longs.constrainToRange(cdnTTL, 0, MAX_CDN_TTL);
      this.serviceTTL = Longs.constrainToRange(serviceTTL, 0, MAX_SERVICE_TTL);
      this.staticTTL = staticTTL;
      this.contentUpdatedAt = contentUpdatedAt;
    }
  }

  public Map<String,Object> asMap() {
    try {
      //noinspection unchecked
      return Json.decodeValue(DatabindCodec.mapper().writerWithView(Static.class).writeValueAsString(this), Map.class);
    } catch (Exception e) {
      return Collections.emptyMap();
    }
  }

  /**
   * Used for logging purposes.
   * @return
   */
  @Override
  public String toString() {
    return Json.encode(this);
  }
}
