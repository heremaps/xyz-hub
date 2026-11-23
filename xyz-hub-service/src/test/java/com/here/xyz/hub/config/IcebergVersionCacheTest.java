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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.xyz.hub.config.IcebergVersionCache.IcebergVersion;
import com.here.xyz.hub.config.IcebergVersionCache.InvalidateIcebergVersionsMessage;
import com.here.xyz.models.hub.DataReference;
import com.here.xyz.models.hub.IcebergLocation;
import com.here.xyz.models.hub.Space;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IcebergVersionCacheTest {

  private static final String SPACE_ID = "iceberg-version-cache-test";
  private static final long SPACE_CREATED_AT = 1_700_000_000_000L;
  private static final Predicate<IcebergVersion> ANY = v -> true;

  private final AtomicLong clock = new AtomicLong(SPACE_CREATED_AT + 1_000_000L);
  private final List<String> broadcasts = new ArrayList<>();
  private final Queue<Future<List<DataReference>>> loadResults = new LinkedList<>();
  private final AtomicInteger loads = new AtomicInteger();
  private final Space space = new Space().withId(SPACE_ID).withCreatedAt(SPACE_CREATED_AT);
  private DataReferenceConfigClient references;
  private IcebergVersionCache cache;

  @BeforeEach
  void setUp() {
    references = mock(DataReferenceConfigClient.class);
    when(references.load(anyString(), isNull(), isNull(), isNull(), isNull(), isNull(), eq(IcebergVersionCache.TARGET_SYSTEM)))
        .thenAnswer(invocation -> {
          loads.incrementAndGet();
          Future<List<DataReference>> result = loadResults.poll();
          return result != null ? result : Future.succeededFuture(List.of());
        });
    cache = newCache(IcebergVersionCache.TTL_MILLIS);
  }

  private IcebergVersionCache newCache(long ttlMillis) {
    return new IcebergVersionCache(() -> references, clock::get, IcebergVersionCache.MAX_SIZE, ttlMillis, broadcasts::add);
  }

  @Test
  void lookupBuildsTheVersionsFromTheReferences() {
    DataReference v1 = ref(1, "t1");
    DataReference v2a = ref(2, "t2a");
    DataReference v2b = ref(2, "t2b");
    DataReference noEndVersion = ref(3, "t3").withEndVersion(null);
    DataReference invalidLocation = ref(4, "t4").withLocation("s3://bucket/key");
    loadResults.add(Future.succeededFuture(List.of(v1, v2a, v2b, noEndVersion, invalidLocation)));

    assertThat(lookup(1)).map(IcebergVersion::referenceId).contains(v1.getId());
    assertThat(lookup(1)).map(IcebergVersion::location).contains(IcebergLocation.parse(v1.getLocation()));
    //Same createdAt, so the higher reference ID wins
    UUID expected = v2a.getId().compareTo(v2b.getId()) > 0 ? v2a.getId() : v2b.getId();
    assertThat(lookup(2)).map(IcebergVersion::referenceId).contains(expected);
    assertThat(lookup(3)).isEmpty();
    assertThat(lookup(4)).isEmpty();
    assertThat(lookup(5)).isEmpty();
    assertThat(loads).hasValue(1);
  }

  @Test
  void emptyResultIsCached() {
    assertThat(lookup(1)).isEmpty();
    assertThat(lookup(1)).isEmpty();
    assertThat(loads).hasValue(1);
  }

  @Test
  void concurrentLookupsTriggerOneLoad() {
    Promise<List<DataReference>> load = Promise.promise();
    loadResults.add(load.future());
    DataReference v1 = ref(1, "t1");

    Future<Optional<IcebergVersion>> first = cache.lookup(null, space, 1, ANY);
    Future<Optional<IcebergVersion>> second = cache.lookup(null, space, 1, ANY);
    assertThat(first.isComplete()).isFalse();
    assertThat(second.isComplete()).isFalse();

    load.complete(List.of(v1));

    assertThat(first.result()).map(IcebergVersion::referenceId).contains(v1.getId());
    assertThat(second.result()).map(IcebergVersion::referenceId).contains(v1.getId());
    assertThat(loads).hasValue(1);
  }

  @Test
  void invalidateRemovesBroadcastsAndForcesAReload() {
    DataReference v1 = ref(1, "t1");
    loadResults.add(Future.succeededFuture(List.of()));
    loadResults.add(Future.succeededFuture(List.of(v1)));

    assertThat(lookup(1)).isEmpty();
    cache.invalidate(SPACE_ID);

    assertThat(broadcasts).containsExactly(SPACE_ID);
    assertThat(lookup(1)).map(IcebergVersion::referenceId).contains(v1.getId());
    assertThat(loads).hasValue(2);
  }

  @Test
  void loadInFlightDuringInvalidateIsNotStored() {
    Promise<List<DataReference>> staleLoad = Promise.promise();
    loadResults.add(staleLoad.future());
    DataReference v1 = ref(1, "t1");
    loadResults.add(Future.succeededFuture(List.of(v1)));

    Future<Optional<IcebergVersion>> inFlight = cache.lookup(null, space, 1, ANY);
    cache.invalidate(SPACE_ID);
    staleLoad.complete(List.of());

    assertThat(inFlight.result()).isEmpty();
    assertThat(lookup(1)).map(IcebergVersion::referenceId).contains(v1.getId());
    assertThat(loads).hasValue(2);
  }

  @Test
  void expiredReferencesAreIgnored() {
    DataReference expired = ref(1, "expired").withKeepUntil(clock.get() - 1);
    DataReference keptUntilNow = ref(2, "now").withKeepUntil(clock.get());
    loadResults.add(Future.succeededFuture(List.of(expired, keptUntilNow)));

    assertThat(lookup(1)).isEmpty();
    assertThat(lookup(2)).map(IcebergVersion::referenceId).contains(keptUntilNow.getId());
  }

  @Test
  void referencesCreatedBeforeTheSpaceAreIgnored() {
    DataReference older = ref(1, "older").withCreatedAt(SPACE_CREATED_AT - 1);
    DataReference unknown = ref(2, "unknown").withCreatedAt(null);
    DataReference same = ref(3, "same").withCreatedAt(SPACE_CREATED_AT);
    loadResults.add(Future.succeededFuture(List.of(older, unknown, same)));

    assertThat(lookup(1)).isEmpty();
    assertThat(lookup(2)).isEmpty();
    assertThat(lookup(3)).map(IcebergVersion::referenceId).contains(same.getId());
  }

  @Test
  void newestReferenceWinsForDuplicateVersions() {
    DataReference oldest = ref(1, "oldest").withCreatedAt(SPACE_CREATED_AT + 10);
    DataReference newest = ref(1, "newest").withCreatedAt(SPACE_CREATED_AT + 30);
    DataReference middle = ref(1, "middle").withCreatedAt(SPACE_CREATED_AT + 20);
    loadResults.add(Future.succeededFuture(List.of(oldest, newest, middle)));

    assertThat(lookup(1)).map(IcebergVersion::referenceId).contains(newest.getId());
  }

  @Test
  void predicateIsApplied() {
    DataReference older = ref(1, "older").withCreatedAt(SPACE_CREATED_AT + 10);
    DataReference newer = ref(1, "newer").withCreatedAt(SPACE_CREATED_AT + 20);
    loadResults.add(Future.succeededFuture(List.of(older, newer)));

    Optional<IcebergVersion> result = cache.lookup(null, space, 1, v -> v.location().table().equals("older")).result();

    assertThat(result).map(IcebergVersion::referenceId).contains(older.getId());
    assertThat(cache.lookup(null, space, 1, v -> false).result()).isEmpty();
  }

  @Test
  void entriesExpireAfterTheTtl() throws InterruptedException {
    //ExpiringMap uses its own clock, so the expiry is checked with a short TTL
    cache = newCache(50);
    lookup(1);
    long deadline = System.currentTimeMillis() + 5_000;
    while (loads.get() < 2 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
      lookup(1);
    }
    assertThat(loads).hasValue(2);
  }

  @Test
  void loadFailureIsNotCached() {
    RuntimeException failure = new RuntimeException("load failure");
    loadResults.add(Future.failedFuture(failure));

    Future<Optional<IcebergVersion>> failed = cache.lookup(null, space, 1, ANY);

    assertThat(failed.failed()).isTrue();
    assertThat(failed.cause()).isSameAs(failure);
    assertThat(lookup(1)).isEmpty();
    assertThat(loads).hasValue(2);
  }

  @Test
  void invalidateIfIcebergIgnoresOtherTargetSystems() {
    cache.invalidateIfIceberg(ref(1, "t1").withTargetSystem("POSTGRES"));
    cache.invalidateIfIceberg(ref(1, "t1").withTargetSystem(null));
    cache.invalidateIfIceberg(ref(1, "t1").withEntityId(null));
    assertThat(broadcasts).isEmpty();
    assertThat(cache.generation(SPACE_ID)).isZero();

    cache.invalidateIfIceberg(ref(1, "t1"));
    assertThat(broadcasts).containsExactly(SPACE_ID);
    assertThat(cache.generation(SPACE_ID)).isEqualTo(1);
  }

  @Test
  void invalidateMessageRemovesLocallyWithoutBroadcasting() {
    String spaceId = SPACE_ID + "-" + UUID.randomUUID();
    InvalidateIcebergVersionsMessage message = new InvalidateIcebergVersionsMessage().withId(spaceId);

    message.handleAtDestination();

    //The singleton's broadcaster would need a message broker, which unit tests don't have
    assertThat(IcebergVersionCache.getInstance().generation(spaceId)).isEqualTo(1);
    assertThat(message.getId()).isEqualTo(spaceId);

    loadResults.add(Future.succeededFuture(List.of()));
    lookup(1);
    cache.invalidateLocally(SPACE_ID);
    assertThat(broadcasts).isEmpty();
    lookup(1);
    assertThat(loads).hasValue(2);
  }

  private Optional<IcebergVersion> lookup(long version) {
    Future<Optional<IcebergVersion>> result = cache.lookup(null, space, version, ANY);
    assertThat(result.succeeded()).isTrue();
    return result.result();
  }

  private DataReference ref(int endVersion, String table) {
    return new DataReference()
        .withId(UUID.randomUUID())
        .withEntityId(SPACE_ID)
        .withStartVersion(0)
        .withEndVersion(endVersion)
        .withTargetSystem(IcebergVersionCache.TARGET_SYSTEM)
        .withLocation(new IcebergLocation("ns", table, "v" + endVersion).toString())
        .withCreatedAt(SPACE_CREATED_AT + 1);
  }
}
