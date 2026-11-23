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

package com.here.xyz.hub.task;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.hub.config.SpaceConfigClient.ClearResult.ABSENT;
import static com.here.xyz.hub.config.SpaceConfigClient.ClearResult.CONDITION_FAILED;
import static com.here.xyz.hub.config.SpaceConfigClient.ClearResult.REMOVED;
import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient.ClearResult;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LazyTableClearingTest {

  private static final Marker MARKER = MarkerManager.getMarker("LazyTableClearingTest");

  private SpaceConfigClient previousSpaceConfigClient;
  private SpaceConfigClient spaceConfigClient;

  @BeforeEach
  void setUp() {
    previousSpaceConfigClient = Service.spaceConfigClient;
    spaceConfigClient = mock(SpaceConfigClient.class);
    when(spaceConfigClient.clearLazyTable(any(), anyString(), anyString(), any(), anyString()))
        .thenReturn(Future.succeededFuture(REMOVED));
    Service.spaceConfigClient = spaceConfigClient;
  }

  @AfterEach
  void tearDown() {
    Service.spaceConfigClient = previousSpaceConfigClient;
  }

  @Test
  void takeMaterializedReturnsTheValueAndRemovesTheField() {
    FeatureCollection materialized = new FeatureCollection().withMaterialized(true);
    FeatureCollection notMaterialized = new FeatureCollection().withMaterialized(false);
    FeatureCollection withoutField = new FeatureCollection();

    assertThat(LazyTableClearing.takeMaterialized(materialized)).isTrue();
    assertThat(LazyTableClearing.takeMaterialized(notMaterialized)).isFalse();
    assertThat(LazyTableClearing.takeMaterialized(withoutField)).isFalse();
    assertThat(materialized.getMaterialized()).isNull();
    assertThat(notMaterialized.getMaterialized()).isNull();
    assertThat(withoutField.getMaterialized()).isNull();
  }

  @Test
  void takeMaterializedIgnoresOtherResponses() {
    assertThat(LazyTableClearing.takeMaterialized(new SuccessResponse())).isFalse();
    assertThat(LazyTableClearing.takeMaterialized(null)).isFalse();
  }

  @Test
  void writeTargetIsTheSpaceExceptForSuper() {
    Space base = space("base", "psql", "base_table", true);
    Space extension = extending(space("extension", "psql", "extension_table", true), base);

    assertThat(LazyTableClearing.writeTarget(extension, DEFAULT)).isSameAs(extension);
    assertThat(LazyTableClearing.writeTarget(extension, EXTENSION)).isSameAs(extension);
    assertThat(LazyTableClearing.writeTarget(extension, null)).isSameAs(extension);
    assertThat(LazyTableClearing.writeTarget(extension, SUPER)).isSameAs(base);
    assertThat(LazyTableClearing.writeTarget(base, SUPER)).isNull();
  }

  @Test
  void contentUpdateIsDeferredForAFlaggedOwnWriteTargetOnly() {
    Space lazy = space("lazy", "psql", "lazy_table", true);
    Space eager = space("eager", "psql", "eager_table", false);

    assertThat(LazyTableClearing.defersContentUpdate(lazy, DEFAULT)).isTrue();
    assertThat(LazyTableClearing.defersContentUpdate(lazy, EXTENSION)).isTrue();
    assertThat(LazyTableClearing.defersContentUpdate(lazy, null)).isTrue();
    assertThat(LazyTableClearing.defersContentUpdate(lazy, SUPER)).isFalse();
    assertThat(LazyTableClearing.defersContentUpdate(eager, DEFAULT)).isFalse();
  }

  @Test
  void noClearWithoutMaterialized() {
    assertThat(await(LazyTableClearing.clearAfterWrite(MARKER, space("lazy", "psql", "lazy_table", true), DEFAULT, false))).isNull();
    verifyNoInteractions(spaceConfigClient);
  }

  @Test
  void noClearForATargetWithoutFlagInTheHubsConfig() {
    Space base = space("base", "psql", "base_table", false);
    Space extension = extending(space("extension", "psql", "extension_table", true), base);

    assertThat(await(LazyTableClearing.clearAfterWrite(MARKER, space("eager", "psql", "eager_table", false), DEFAULT, true))).isNull();
    assertThat(await(LazyTableClearing.clearAfterWrite(MARKER, extension, SUPER, true))).isNull();
    assertThat(await(LazyTableClearing.clearAfterWrite(MARKER, space("base", "psql", "base_table", true), SUPER, true))).isNull();
    verifyNoInteractions(spaceConfigClient);
  }

  @Test
  void clearPassesTheConnectorAndTheTableNameOfTheTarget() {
    assertThat(await(LazyTableClearing.clearAfterWrite(MARKER, space("lazy", "psql", "lazy_table", true), DEFAULT, true)))
        .isEqualTo(REMOVED);
    verify(spaceConfigClient).clearLazyTable(MARKER, "lazy", "psql", "lazy_table", LazyTableClearing.SOURCE_CONNECTOR_REPORT);
  }

  @Test
  void clearOfALegacyTargetPassesNoTableName() {
    LazyTableClearing.clearAfterWrite(MARKER, space("legacy", "psql", null, true), EXTENSION, true);
    verify(spaceConfigClient).clearLazyTable(MARKER, "legacy", "psql", null, LazyTableClearing.SOURCE_CONNECTOR_REPORT);
  }

  @Test
  void superWriteClearsTheBaseWithTheConnectorOfTheComposite() {
    Space base = space("base", "psql", "base_table", true);
    Space extension = extending(space("extension", "psql", "extension_table", true), base);

    LazyTableClearing.clearAfterWrite(MARKER, extension, SUPER, true);
    verify(spaceConfigClient).clearLazyTable(MARKER, "base", "psql", "base_table", LazyTableClearing.SOURCE_CONNECTOR_REPORT);
  }

  @Test
  void aThrowingClientGivesAFailedFuture() {
    when(spaceConfigClient.clearLazyTable(any(), anyString(), anyString(), any(), anyString()))
        .thenThrow(new IllegalStateException("broken"));

    Future<ClearResult> clear = LazyTableClearing.clearAfterWrite(MARKER, space("lazy", "psql", "lazy_table", true), DEFAULT, true);
    assertThat(clear.failed()).isTrue();
    assertThat(clear.cause()).hasMessage("broken");
  }

  @Test
  void isClearedForRemovedAndAbsent() {
    assertThat(LazyTableClearing.isCleared(REMOVED)).isTrue();
    assertThat(LazyTableClearing.isCleared(ABSENT)).isTrue();
    assertThat(LazyTableClearing.isCleared(CONDITION_FAILED)).isFalse();
    assertThat(LazyTableClearing.isCleared(null)).isFalse();
  }

  @Test
  void validClearParamsPass() {
    assertThatCode(() -> LazyTableClearing.validateClearParams("space", "psql", "table")).doesNotThrowAnyException();
    assertThatCode(() -> LazyTableClearing.validateClearParams("space", "psql", null)).doesNotThrowAnyException();
    assertThatCode(() -> LazyTableClearing.validateClearParams("space", "c".repeat(256), "t".repeat(63)))
        .doesNotThrowAnyException();
  }

  @Test
  void invalidClearParamsGive400() {
    assertBadRequest(null, "psql", "table");
    assertBadRequest("", "psql", "table");
    assertBadRequest("space", null, "table");
    assertBadRequest("space", "", "table");
    assertBadRequest("space", "c".repeat(257), "table");
    assertBadRequest("space", "psql", "");
    assertBadRequest("space", "psql", "xyz_empty_space_v1");
    assertBadRequest("space", "psql", "t".repeat(64));
    assertBadRequest("space", "psql", "a\0b");
  }

  private static void assertBadRequest(String spaceId, String connector, String table) {
    assertThatThrownBy(() -> LazyTableClearing.validateClearParams(spaceId, connector, table))
        .isInstanceOfSatisfying(HttpException.class, e -> assertThat(e.status).isEqualTo(BAD_REQUEST));
  }

  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().join();
  }

  private static Space space(String id, String storageId, String tableName, boolean lazy) {
    Map<String, Object> params = new HashMap<>();
    if (tableName != null)
      params.put(TABLE_NAME, tableName);
    if (lazy)
      params.put(LAZY_TABLE, true);
    Space space = new Space();
    space.setId(id);
    space.setStorage(new ConnectorRef().withId(storageId).withParams(params));
    return space;
  }

  private static Space extending(Space space, Space base) {
    Extension extension = new Extension().withSpaceId(base.getId());
    extension.resolvedSpace = base;
    space.setExtension(extension);
    return space;
  }
}
