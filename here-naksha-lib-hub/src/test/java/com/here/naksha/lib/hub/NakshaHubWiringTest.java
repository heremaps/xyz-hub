/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.hub;

import static com.here.naksha.lib.common.TestFileLoader.parseJsonFileOrFail;
import static com.here.naksha.lib.common.TestNakshaContext.newTestNakshaContext;
import static com.here.naksha.lib.core.util.storage.RequestHelper.createFeatureRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.EndPipelineHandler;
import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.models.PluginCache;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.IfConflict;
import com.here.naksha.lib.core.models.storage.IfExists;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.models.storage.WriteXyzCollections;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.models.storage.XyzCodecFactory;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.handlers.AuthorizationEventHandler;
import com.here.naksha.lib.handlers.DefaultStorageHandler;
import com.here.naksha.lib.handlers.IntHandlerForStorages;
import com.here.naksha.lib.hub.mock.MockResult;
import com.here.naksha.lib.hub.storages.NHAdminStorage;
import com.here.naksha.lib.hub.storages.NHAdminStorageReader;
import com.here.naksha.lib.hub.storages.NHAdminStorageWriter;
import com.here.naksha.lib.hub.storages.NHSpaceStorage;
import com.here.naksha.lib.psql.PsqlStorage.Params;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NakshaHubWiringTest {

  @Mock
  static NakshaHub hub;

  @Mock
  static NHAdminStorage adminStorage;

  @Mock
  static NHAdminStorageReader adminStorageReader;

  @Mock
  static NHAdminStorageWriter adminStorageWriter;

  static NakshaEventPipelineFactory spyPipelineFactory;

  static NHSpaceStorage spaceStorage = null;

  @BeforeEach
  void beforeEachTest() throws Exception {
    MockitoAnnotations.openMocks(this);
    spyPipelineFactory = spy(new NakshaEventPipelineFactory(hub));
    spaceStorage = new NHSpaceStorage(hub, spyPipelineFactory);
    when(hub.getSpaceStorage()).thenReturn(spaceStorage);
    when(hub.getAdminStorage()).thenReturn(adminStorage);
    when(adminStorage.newReadSession(any(), anyBoolean())).thenReturn(adminStorageReader);
    when(adminStorage.newWriteSession(any(), anyBoolean())).thenReturn(adminStorageWriter);
  }

  private <T> T parseJson(final @NotNull String jsonStr, final @NotNull Class<T> type) throws Exception {
    T obj = null;
    try (final Json json = Json.get()) {
      obj = json.reader(ViewDeserialize.Storage.class).forType(type).readValue(jsonStr);
    }
    return obj;
  }

  @Test
  @Order(1)
  void testCreateStorageRequestWiring() {
    // Given: Create Storage request
    final Storage storage = parseJsonFileOrFail("create_storage.json", Storage.class);
    final WriteXyzFeatures request =
        createFeatureRequest(NakshaAdminCollection.STORAGES, storage, IfExists.REPLACE, IfConflict.REPLACE);

    // And: spies and captors in place
    final EventPipeline spyPipeline = spy(spyPipelineFactory.eventPipeline());
    when(spyPipelineFactory.eventPipeline()).thenReturn(spyPipeline);
    final ArgumentCaptor<WriteRequest<?, ?, ?>> reqCaptor = ArgumentCaptor.forClass(WriteRequest.class);
    final ArgumentCaptor<IEventHandler> handlerCaptor = ArgumentCaptor.forClass(IEventHandler.class);

    // When: Request is submitted to Hub Space Storage
    try (final IWriteSession admin = hub.getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      admin.execute(request);
      admin.commit(true);
    }

    // Then:
    // Verify: 2 event pipelines created (1 actual + 1 due to spy setup)
    verify(spyPipelineFactory, times(2)).eventPipeline();
    // Verify: 3 known event handlers are added to the pipeline
    verify(spyPipeline, times(3)).addEventHandler(handlerCaptor.capture());
    final List<IEventHandler> handlers = handlerCaptor.getAllValues();
    assertTrue(
        handlers.get(0) instanceof AuthorizationEventHandler, "Expected instance of AuthorizationEventHandler");
    assertTrue(handlers.get(1) instanceof IntHandlerForStorages, "Expected instance of IntHandlerForStorages");
    assertTrue(handlers.get(2) instanceof EndPipelineHandler, "Expected instance of EndPipelineHandler");
    // Verify: admin storage writer finally gets the write request
    verify(adminStorageWriter, times(1)).execute(reqCaptor.capture());
    assertTrue(reqCaptor.getValue() instanceof WriteFeatures<?, ?, ?>);
  }

  @Test
  @Order(2)
  void testGetStoragesRequestWiring() throws Exception {
    // Given: Read Storage request
    final ReadFeatures request = new ReadFeatures(NakshaAdminCollection.STORAGES);

    // And: spies and captors in place
    final EventPipeline spyPipeline = spy(spyPipelineFactory.eventPipeline());
    when(spyPipelineFactory.eventPipeline()).thenReturn(spyPipeline);
    final ArgumentCaptor<ReadRequest<?>> reqCaptor = ArgumentCaptor.forClass(ReadRequest.class);
    final ArgumentCaptor<IEventHandler> handlerCaptor = ArgumentCaptor.forClass(IEventHandler.class);

    // When: Request is submitted to Hub Space Storage
    try (final IReadSession reader = hub.getSpaceStorage().newReadSession(newTestNakshaContext(), true)) {
      reader.execute(request);
    }

    // Then:
    // Verify: 2 event pipelines created (1 actual + 1 due to spy setup)
    verify(spyPipelineFactory, times(2)).eventPipeline();
    // Verify: 3 known event handlers are added to the pipeline
    verify(spyPipeline, times(3)).addEventHandler(handlerCaptor.capture());
    final List<IEventHandler> handlers = handlerCaptor.getAllValues();
    assertTrue(
        handlers.get(0) instanceof AuthorizationEventHandler, "Expected instance of AuthorizationEventHandler");
    assertTrue(handlers.get(1) instanceof IntHandlerForStorages, "Expected instance of IntHandlerForStorages");
    assertTrue(handlers.get(2) instanceof EndPipelineHandler, "Expected instance of EndPipelineHandler");
    // Verify: admin storage writer finally gets the write request
    verify(adminStorageReader, times(1)).execute(reqCaptor.capture());
    assertTrue(reqCaptor.getValue() instanceof ReadFeatures);
  }

  @Test
  @Order(3)
  void testCreateFeatureRequestWiring() throws Exception {
    // Given: Storage, EventHandler and Space objects
    final Storage storage = parseJsonFileOrFail("createFeature/create_storage.json", Storage.class);
    final EventHandler eventHandler =
        parseJsonFileOrFail("createFeature/create_event_handler.json", EventHandler.class);
    final Space space = parseJsonFileOrFail("createFeature/create_space.json", Space.class);
    final IStorage storageImpl = PluginCache.getStorageConstructor(storage.getClassName(), Storage.class)
        .call(storage);
    storageImpl.initStorage(new Params().pg_hint_plan(false).pg_stat_statements(false));

    // And: mock in place to return given Storage, EventHandler and Space objects, when requested from Admin Storage
    final IStorage spyStorageImpl = spy(storageImpl);
    when(adminStorageReader.execute(argThat(readRequest -> {
          if (readRequest instanceof ReadFeatures rr) {
            return rr.getCollections().get(0).equals(NakshaAdminCollection.SPACES);
          }
          return false;
        })))
        .thenReturn(new MockResult<>(Space.class, List.of(featureCodec(space))));
    when(adminStorageReader.execute(argThat(readRequest -> {
          if (readRequest instanceof ReadFeatures rr) {
            return rr.getCollections().get(0).equals(NakshaAdminCollection.EVENT_HANDLERS);
          }
          return false;
        })))
        .thenReturn(new MockResult<>(EventHandler.class, List.of(featureCodec(eventHandler))));
    when(hub.getStorageById(argThat(argument -> argument.equals(storage.getId()))))
        .thenReturn(spyStorageImpl);
    // And: setup spy on Custom Storage Writer to intercept execute() method calls
    final IWriteSession spyWriter = spy(spyStorageImpl.newWriteSession(newTestNakshaContext(), true));
    doReturn(spyWriter).when(spyStorageImpl).newWriteSession(any(), anyBoolean());

    // And: Create Feature request
    final XyzFeature feature = parseJsonFileOrFail("createFeature/create_feature.json", XyzFeature.class);
    final WriteXyzFeatures request = createFeatureRequest(space.getId(), feature, IfExists.FAIL, IfConflict.FAIL);

    // And: spies and captors in place to return
    final EventPipeline spyPipeline = spy(spyPipelineFactory.eventPipeline());
    when(spyPipelineFactory.eventPipeline()).thenReturn(spyPipeline);
    final ArgumentCaptor<WriteRequest> reqCaptor = ArgumentCaptor.forClass(WriteRequest.class);
    final ArgumentCaptor<IEventHandler> handlerCaptor = ArgumentCaptor.forClass(IEventHandler.class);

    // When: Request is submitted to Hub Space Storage
    try (final IWriteSession writer = hub.getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      final Result result = writer.execute(request);
      writer.commit(true);
    }

    // Then:
    // Verify: 2 event pipelines created (1 actual + 1 due to spy setup)
    verify(spyPipelineFactory, times(2)).eventPipeline();
    // Verify: 3 known event handlers are added to the pipeline
    verify(spyPipeline, times(3)).addEventHandler(handlerCaptor.capture());
    final List<IEventHandler> handlers = handlerCaptor.getAllValues();
    assertTrue(
        handlers.get(0) instanceof AuthorizationEventHandler, "Expected instance of AuthorizationEventHandler");
    assertTrue(handlers.get(1) instanceof DefaultStorageHandler, "Expected instance of DefaultStorageHandler");
    assertTrue(handlers.get(2) instanceof EndPipelineHandler, "Expected instance of EndPipelineHandler");
    // Verify: admin storage writer finally gets:
    //    3 write requests (create feature, create missing collection, reattempt create feature)
    //    + 1 for setting up spy
    verify(spyStorageImpl, times(4)).newWriteSession(any(), anyBoolean());
    verify(spyWriter, times(3)).execute(reqCaptor.capture());
    assertTrue(reqCaptor.getValue() instanceof WriteFeatures);
    final List<WriteRequest> requests = reqCaptor.getAllValues();
    final String collectionId = ((Map) space.getProperties().get("collection"))
        .get("id")
        .toString(); // TODO: this is ambiguous (see Space::getCollectionId), discuss
    // Verify: WriteFeature into collection got called
    assertTrue(requests.get(0) instanceof WriteXyzFeatures, "Expected WriteXyzFeatures type of request.");
    assertEquals(
        collectionId,
        ((WriteXyzFeatures) requests.get(0)).getCollectionId(),
        "CollectionId mismatch for Write Feature request");
    // Verify: WriteCollection got called (to create missing table)
    assertTrue(requests.get(1) instanceof WriteXyzCollections, "Expected WriteXyzCollections type of request.");
    assertEquals(
        collectionId,
        ((WriteXyzCollections) requests.get(1))
            .features
            .get(0)
            .getFeature()
            .getId(),
        "CollectionId mismatch for Write Collection request");
    // Verify: WriteFeature into collectionId got called again
    assertTrue(
        requests.get(2) instanceof WriteXyzFeatures,
        "Expected WriteXyzFeatures type of request during reattempt.");
    assertEquals(
        collectionId,
        ((WriteXyzFeatures) requests.get(2)).getCollectionId(),
        "CollectionId mismatch for reattempted Write Feature request");
  }

  private XyzFeatureCodec featureCodec(XyzFeature feature) {
    return XyzCodecFactory.getFactory(XyzFeatureCodecFactory.class)
        .newInstance()
        .withFeature(feature);
  }
}
