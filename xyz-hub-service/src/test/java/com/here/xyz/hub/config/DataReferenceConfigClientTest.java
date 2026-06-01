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

import com.here.xyz.models.hub.DataReference;
import io.vertx.core.Future;
import org.apache.logging.log4j.Marker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class DataReferenceConfigClientTest {

  private static final String ENTITY_ID = "space-id-test";
  private static final long TWENTY_FOUR_HOURS_MS = TimeUnit.HOURS.toMillis(24);

  @Mock
  private Marker marker;

  private FakeDataReferenceConfigClient client;

  @BeforeEach
  void setUp() {
    client = new FakeDataReferenceConfigClient();
  }

  @Test
  void expireForEntity_shouldComplete_whenNoReferencesExistForEntity() {
    client.loadResult = Future.succeededFuture(List.of());

    Void result = await(client.expireForEntity(marker, ENTITY_ID, TWENTY_FOUR_HOURS_MS));

    assertThat(result).isNull();
    assertThat(client.storedRefs).isEmpty();
    assertThat(client.loadCalls).containsExactly(ENTITY_ID);
  }

  @Test
  void expireForEntity_shouldComplete_whenLoadReturnsNullList() {
    client.loadResult = Future.succeededFuture(null);

    Void result = await(client.expireForEntity(marker, ENTITY_ID, TWENTY_FOUR_HOURS_MS));

    assertThat(result).isNull();
    assertThat(client.storedRefs).isEmpty();
  }

  @Test
  void expireForEntity_shouldSetKeepUntilOnAllReferences() {
    DataReference r1 = newRef(null);
    DataReference r2 = newRef(null);
    DataReference r3 = newRef(null);
    client.loadResult = Future.succeededFuture(List.of(r1, r2, r3));

    long newKeepUntil = TWENTY_FOUR_HOURS_MS;
    await(client.expireForEntity(marker, ENTITY_ID, newKeepUntil));

    assertThat(client.storedRefs).containsExactlyInAnyOrder(r1, r2, r3);
    assertThat(client.storedRefs)
        .extracting(DataReference::getKeepUntil)
        .containsOnly(newKeepUntil);
  }

  @Test
  void expireForEntity_shouldOverwriteExistingKeepUntil() {
    long existingKeepUntil = 1_500_000_000_000L;
    DataReference r = newRef(existingKeepUntil);
    client.loadResult = Future.succeededFuture(List.of(r));

    long newKeepUntil = 2_000_000_000_000L;
    await(client.expireForEntity(marker, ENTITY_ID, newKeepUntil));

    assertThat(client.storedRefs).hasSize(1);
    assertThat(client.storedRefs.get(0).getKeepUntil()).isEqualTo(newKeepUntil);
  }

  @Test
  void expireForEntity_shouldUseGivenTime() {
    DataReference r = newRef(null);
    client.loadResult = Future.succeededFuture(List.of(r));

    long now = System.currentTimeMillis();
    long target = now + TWENTY_FOUR_HOURS_MS;
    await(client.expireForEntity(marker, ENTITY_ID, target));

    assertThat(client.storedRefs).hasSize(1);
    Long stored = client.storedRefs.get(0).getKeepUntil();
    assertThat(stored).isNotNull();

    assertThat(stored - now).isEqualTo(TWENTY_FOUR_HOURS_MS);
    assertThat(stored - now).isEqualTo(86_400_000L);
  }

  @Test
  void expireForEntity_shouldPropagateLoadFailure() {
    RuntimeException failed = new RuntimeException("load failure");
    client.loadResult = Future.failedFuture(failed);

    assertThatThrownBy(() -> await(client.expireForEntity(marker, ENTITY_ID, 1L)))
        .isInstanceOf(CompletionException.class)
        .hasCause(failed);
    assertThat(client.storedRefs).isEmpty();
  }

  @Test
  void expireForEntity_shouldFailFuture_whenOneStoreFails() {
    DataReference r1 = newRef(null);
    DataReference r2 = newRef(null);
    client.loadResult = Future.succeededFuture(List.of(r1, r2));

    RuntimeException storeFailure = new RuntimeException("store failure");
    client.storeFunction = reference -> reference == r2 ? Future.failedFuture(storeFailure) : Future.succeededFuture(reference.getId());

    assertThatThrownBy(() -> await(client.expireForEntity(marker, ENTITY_ID, 1L)))
        .isInstanceOf(CompletionException.class)
        .hasCause(storeFailure);

    assertThat(client.storedRefs).contains(r1);
  }

  @Test
  void expireForEntity_shouldRejectNullEntityId() {
    assertThatNullPointerException()
        .isThrownBy(() -> client.expireForEntity(marker, null, 1L));
  }


  private static DataReference newRef(Long keepUntil) {
    return new DataReference()
        .withId(UUID.randomUUID())
        .withEntityId(DataReferenceConfigClientTest.ENTITY_ID)
        .withKeepUntil(keepUntil);
  }

  private static <T> T await(Future<T> f) {
    return f.toCompletionStage().toCompletableFuture().join();
  }

  private static final class FakeDataReferenceConfigClient extends DataReferenceConfigClient {

    Future<List<DataReference>> loadResult = Future.succeededFuture(List.of());

    Function<DataReference, Future<UUID>> storeFunction = ref -> Future.succeededFuture(ref.getId());

    final List<String> loadCalls = new ArrayList<>();
    final List<Object[]> loadFilters = new ArrayList<>();
    final List<DataReference> storedRefs = new ArrayList<>();

    @Override
    protected Future<UUID> doStore(DataReference dataReference) {
      storedRefs.add(dataReference);
      return storeFunction.apply(dataReference);
    }

    @Override
    protected Future<Optional<DataReference>> doLoad(UUID id) {
      return Future.succeededFuture(Optional.empty());
    }

    @Override
    protected Future<List<DataReference>> doLoad(
        String entityId,
        Integer startVersion,
        Integer endVersion,
        String contentType,
        String objectType,
        String sourceSystem,
        String targetSystem
    ) {
      loadCalls.add(entityId);
      loadFilters.add(new Object[] {startVersion, endVersion, contentType, objectType, sourceSystem, targetSystem});
      return loadResult;
    }

    @Override
    protected Future<Void> doDelete(UUID id) {
      return Future.succeededFuture();
    }

    @Override
    public Future<Void> init() {
      return Future.succeededFuture();
    }
  }
}
