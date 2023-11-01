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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import com.here.naksha.lib.core.*;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.handlers.AuthorizationEventHandler;
import com.here.naksha.lib.handlers.IntHandlerForStorages;
import com.here.naksha.lib.hub.storages.NHAdminStorage;
import com.here.naksha.lib.hub.storages.NHAdminStorageReader;
import com.here.naksha.lib.hub.storages.NHAdminStorageWriter;
import com.here.naksha.lib.hub.storages.NHSpaceStorage;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class NakshaHubWiringTest {
  static final String TEST_DATA_FOLDER = "src/test/resources/unit_test_data/";

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
  void setup() throws Exception {
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
  void testCreateStorageRequestWiring() throws Exception {
    // Given: Create Storage request
    final Storage storage = parseJsonFileOrFail("create_storage.json", Storage.class);
    final WriteFeatures<?> request =
        createFeatureRequest(NakshaAdminCollection.STORAGES, storage, IfExists.REPLACE, IfConflict.REPLACE);

    // And: spies and captors in place
    final EventPipeline spyPipeline = spy(spyPipelineFactory.eventPipeline());
    when(spyPipelineFactory.eventPipeline()).thenReturn(spyPipeline);
    final ArgumentCaptor<WriteRequest> reqCaptor = ArgumentCaptor.forClass(WriteRequest.class);
    final ArgumentCaptor<IEventHandler> handlerCaptor = ArgumentCaptor.forClass(IEventHandler.class);

    // When: Request is submitted to Hub Space Storage
    try (final IWriteSession admin = hub.getSpaceStorage().newWriteSession(newTestNakshaContext(), true)) {
      admin.execute(request);
      admin.commit();
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
    assertTrue(reqCaptor.getValue() instanceof WriteFeatures<?>);
  }

  @Test
  void testGetStoragesRequestWiring() throws Exception {
    // Given: Read Storage request
    final ReadFeatures request = new ReadFeatures(NakshaAdminCollection.STORAGES);

    // And: spies and captors in place
    final EventPipeline spyPipeline = spy(spyPipelineFactory.eventPipeline());
    when(spyPipelineFactory.eventPipeline()).thenReturn(spyPipeline);
    final ArgumentCaptor<ReadRequest> reqCaptor = ArgumentCaptor.forClass(ReadRequest.class);
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
}
