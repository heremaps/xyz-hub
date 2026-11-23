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

package com.here.xyz.hub.config;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import com.here.xyz.models.hub.DataReference;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * A local cache of the Iceberg versions of each space, built from the space's data references with the target system
 * {@value #TARGET_SYSTEM}. An entry maps each end version to the references that cover it. Spaces without such references get
 * an empty entry, so that they are not loaded again until the entry expires or gets invalidated.
 * Storing, deleting or expiring an Iceberg reference through the Hub invalidates the entry on all Hub instances.
 */
public class IcebergVersionCache {

  private static final Logger logger = LogManager.getLogger();

  public static final String TARGET_SYSTEM = "ICEBERG";
  static final int MAX_SIZE = 10_000;
  static final long TTL_MILLIS = TimeUnit.MINUTES.toMillis(10);

  private static final Comparator<IcebergVersion> NEWEST = Comparator.comparingLong(IcebergVersion::createdAt)
      .thenComparing(IcebergVersion::referenceId, Comparator.nullsFirst(Comparator.naturalOrder()));

  private final Supplier<DataReferenceConfigClient> references;
  private final LongSupplier clock;
  private final Consumer<String> broadcaster;
  private final ExpiringMap<String, IcebergVersions> cache;
  private final Map<String, Future<IcebergVersions>> pending = new ConcurrentHashMap<>();
  //Bumped by each invalidation of a space; a load only stores its result if the generation did not change while it ran
  private final Map<String, Long> generations = new ConcurrentHashMap<>();

  private final LongAdder hits = new LongAdder();
  private final LongAdder misses = new LongAdder();
  private final LongAdder loads = new LongAdder();
  private final LongAdder invalidations = new LongAdder();

  private static final class InstanceHolder {
    private static final IcebergVersionCache instance = new IcebergVersionCache(DataReferenceConfigClient::getInstance,
        Core::currentTimeMillis, MAX_SIZE, TTL_MILLIS, IcebergVersionCache::broadcastInvalidation);
  }

  public static IcebergVersionCache getInstance() {
    return InstanceHolder.instance;
  }

  IcebergVersionCache(Supplier<DataReferenceConfigClient> references, LongSupplier clock, int maxSize, long ttlMillis,
      Consumer<String> broadcaster) {
    this.references = references;
    this.clock = clock;
    this.broadcaster = broadcaster;
    cache = ExpiringMap.builder()
        .maxSize(maxSize)
        .expirationPolicy(ExpirationPolicy.CREATED)
        .expiration(ttlMillis, TimeUnit.MILLISECONDS)
        .build();
  }

  private static void broadcastInvalidation(String spaceId) {
    if (Service.messageBroker != null)
      new InvalidateIcebergVersionsMessage().withId(spaceId).withGlobalRelay(true).broadcast();
  }

  /**
   * Returns the newest (by {@code createdAt}) Iceberg version of the space for the given version, which is not expired, was not
   * created before the space and matches the filter.
   */
  public Future<Optional<IcebergVersion>> lookup(Marker marker, Space space, long version, Predicate<IcebergVersion> filter) {
    return versions(marker, space.getId()).map(entry -> {
      List<IcebergVersion> candidates = entry.byVersion().get(version);
      if (candidates == null)
        return Optional.empty();
      long now = clock.getAsLong();
      return candidates.stream()
          .filter(v -> v.keepUntil() == null || v.keepUntil() >= now)
          .filter(v -> v.createdAt() >= space.getCreatedAt())
          .filter(filter)
          .max(NEWEST);
    });
  }

  private Future<IcebergVersions> versions(Marker marker, String spaceId) {
    IcebergVersions cached = cache.get(spaceId);
    if (cached != null) {
      hits.increment();
      return Future.succeededFuture(cached);
    }
    misses.increment();

    Promise<IcebergVersions> promise = Promise.promise();
    Future<IcebergVersions> inFlight = pending.putIfAbsent(spaceId, promise.future());
    if (inFlight != null)
      return inFlight;

    long generation = generation(spaceId);
    long start = clock.getAsLong();
    load(spaceId).onComplete(ar -> {
      pending.remove(spaceId, promise.future());
      if (ar.succeeded()) {
        loads.increment();
        if (generation == generation(spaceId))
          cache.put(spaceId, ar.result());
        logger.info(marker, "iceberg-versions space={} entries={} ms={} hits={} misses={} loads={} invalidations={}", spaceId,
            ar.result().byVersion().size(), clock.getAsLong() - start, hits.sum(), misses.sum(), loads.sum(), invalidations.sum());
      }
      else
        logger.warn(marker, "iceberg-versions space={}: Failed to load the Iceberg references", spaceId, ar.cause());
      promise.handle(ar);
    });
    return promise.future();
  }

  private Future<IcebergVersions> load(String spaceId) {
    try {
      return references.get().load(spaceId, null, null, null, null, null, TARGET_SYSTEM).map(refs -> toVersions(spaceId, refs));
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private static IcebergVersions toVersions(String spaceId, List<DataReference> refs) {
    if (refs == null || refs.isEmpty())
      return IcebergVersions.EMPTY;
    TreeMap<Long, List<IcebergVersion>> byVersion = new TreeMap<>();
    for (DataReference ref : refs) {
      if (ref.getEndVersion() == null)
        continue;
      IcebergLocation location;
      try {
        location = IcebergLocation.parse(ref.getLocation());
      }
      catch (IllegalArgumentException e) {
        logger.warn("iceberg-versions space={}: Skipping the reference id={} with an invalid location: {}", spaceId, ref.getId(),
            e.getMessage());
        continue;
      }
      byVersion.computeIfAbsent((long) ref.getEndVersion(), v -> new ArrayList<>())
          .add(new IcebergVersion(ref.getEndVersion(), location, ref.getKeepUntil(),
              ref.getCreatedAt() == null ? 0 : ref.getCreatedAt(), ref.getId()));
    }
    if (byVersion.isEmpty())
      return IcebergVersions.EMPTY;
    byVersion.replaceAll((v, list) -> List.copyOf(list));
    return new IcebergVersions(Collections.unmodifiableNavigableMap(byVersion));
  }

  long generation(String spaceId) {
    return generations.getOrDefault(spaceId, 0L);
  }

  /**
   * Removes the entry of the space on this and, by a broadcast, on all other Hub instances.
   */
  public void invalidate(String spaceId) {
    invalidateLocally(spaceId);
    broadcaster.accept(spaceId);
  }

  /**
   * Invalidates the entry of the reference's space, if the reference is an Iceberg reference.
   */
  public void invalidateIfIceberg(DataReference ref) {
    if (ref != null && TARGET_SYSTEM.equals(ref.getTargetSystem()) && ref.getEntityId() != null)
      invalidate(ref.getEntityId());
  }

  void invalidateLocally(String spaceId) {
    generations.merge(spaceId, 1L, Long::sum);
    cache.remove(spaceId);
    pending.remove(spaceId);
    invalidations.increment();
  }

  /**
   * An Iceberg reference.
   *
   * @param version The end version of the reference
   * @param location The Iceberg table and tag
   * @param keepUntil The expiry of the reference, or {@code null}
   * @param createdAt The creation time of the reference, {@code 0} if unknown
   * @param referenceId The ID of the reference
   */
  public record IcebergVersion(long version, IcebergLocation location, Long keepUntil, long createdAt, UUID referenceId) {}

  /**
   * The Iceberg references of a space, by end version.
   */
  public record IcebergVersions(NavigableMap<Long, List<IcebergVersion>> byVersion) {
    public static final IcebergVersions EMPTY = new IcebergVersions(Collections.emptyNavigableMap());
  }

  public static class InvalidateIcebergVersionsMessage extends RelayedMessage {

    private String id;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public InvalidateIcebergVersionsMessage withId(String id) {
      this.id = id;
      return this;
    }

    @Override
    protected void handleAtDestination() {
      getInstance().invalidateLocally(id);
    }
  }
}
