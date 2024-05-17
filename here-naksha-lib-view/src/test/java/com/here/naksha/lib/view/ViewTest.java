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
package com.here.naksha.lib.view;

import com.here.naksha.lib.core.*;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.exceptions.TooManyTasks;
import com.here.naksha.lib.core.exceptions.UncheckedException;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.view.concurrent.LayerReadRequest;
import com.here.naksha.lib.view.concurrent.ParallelQueryExecutor;
import com.here.naksha.lib.view.merge.MergeByStoragePriority;
import com.here.naksha.lib.view.missing.IgnoreMissingResolver;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.here.naksha.lib.core.models.storage.POp.eq;
import static com.here.naksha.lib.core.models.storage.POp.or;
import static com.here.naksha.lib.core.models.storage.PRef.app_id;
import static com.here.naksha.lib.core.models.storage.PRef.id;
import static com.here.naksha.lib.view.Sample.sampleXyzResponse;
import static com.here.naksha.lib.view.Sample.sampleXyzWriteResponse;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ViewTest {

  private NakshaContext nc =
      new NakshaContext().withAppId("VIEW_API_TEST").withAuthor("VIEW_API_AUTHOR");

  private final static String TOPO = "topologies";

  @Test
  void testReadApiNotation() throws NoCursor {

    // given
    IStorage storage = mock(IStorage.class);
    ViewLayer topologiesDS = new ViewLayer(storage, "topologies");
    ViewLayer buildingsDS = new ViewLayer(storage, "buildings");
    ViewLayer topologiesCS = new ViewLayer(storage, "topologies");

    // each layer is going to return 3 same records
    List<XyzFeatureCodec> results = sampleXyzResponse(3);
    when(storage.newReadSession(nc, false)).thenReturn(new MockReadSession(results));

    ViewLayerCollection viewLayerCollection = new ViewLayerCollection("myCollection", topologiesDS, buildingsDS, topologiesCS);

    View view = new View(viewLayerCollection);

    MergeOperation<XyzFeature, XyzFeatureCodec> customMergeOperation = new MergeByStoragePriority<>();
    MissingIdResolver<XyzFeature, XyzFeatureCodec> skipFetchingResolver = new IgnoreMissingResolver<>();

    // when
    ViewReadSession readSession = view.newReadSession(nc, false);
    ReadFeatures readFeatures = new ReadFeatures();
    Result result = readSession.execute(
        readFeatures, XyzFeatureCodecFactory.get(), customMergeOperation, skipFetchingResolver);
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = result.getXyzMutableCursor();

    // then
    assertTrue(cursor.next());
    List<XyzFeatureCodec> allFeatures = cursor.asList();
    assertEquals(3, allFeatures.size());
    assertTrue(allFeatures.containsAll(results));
  }

  @Test
  void testWriteApiNotation() throws NoCursor {
    IStorage storage = mock(IStorage.class);
    IWriteSession session = mock(IWriteSession.class);

    ViewLayer topologiesDS = new ViewLayer(storage, "topologies");
    ViewLayerCollection viewLayerCollection = new ViewLayerCollection("myCollection", topologiesDS);
    View view = new View(viewLayerCollection);
    when(storage.newWriteSession(nc, true)).thenReturn(session);

    final LayerWriteFeatureRequest request = new LayerWriteFeatureRequest();
    final XyzFeature feature = new XyzFeature("id0");
    request.add(EWriteOp.CREATE, feature);

    when(session.execute(request)).thenReturn(new MockResult<>(sampleXyzWriteResponse(1, EExecutedOp.CREATED)));
    ViewWriteSession writeSession = view.newWriteSession(nc, true);

    try (ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        writeSession.execute(request).getXyzFeatureCursor()) {
      assertTrue(cursor.hasNext());
      cursor.next();
      assertEquals(feature.getId(), cursor.getFeature().getId());
      assertEquals(cursor.getOp(), EExecutedOp.CREATED);
    } finally {
      writeSession.commit(true);
    }
  }

  @Test
  void testDeleteApiNotation() throws NoCursor {
    IStorage storage = mock(IStorage.class);
    IWriteSession session = mock(IWriteSession.class);

    ViewLayer topologiesDS = new ViewLayer(storage, "topologies");
    ViewLayerCollection viewLayerCollection = new ViewLayerCollection("myCollection", topologiesDS);
    View view = new View(viewLayerCollection);
    when(storage.newWriteSession(nc, true)).thenReturn(session);

    final LayerWriteFeatureRequest request = new LayerWriteFeatureRequest();
    final XyzFeature feature = new XyzFeature("id0");
    request.add(EWriteOp.DELETE, feature);

    when(session.execute(request)).thenReturn(new MockResult<>(sampleXyzWriteResponse(1, EExecutedOp.DELETED)));
    ViewWriteSession writeSession = view.newWriteSession(nc, true);

    try (ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        writeSession.execute(request).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      assertEquals(feature.getId(), cursor.getId());
      assertEquals(cursor.getOp(), EExecutedOp.DELETED);
    } finally {
      writeSession.commit(true);
    }
  }

  @Test
  void testExceptionInOneOfTheThreads() {
    // given
    IReadSession readSession = mock(IReadSession.class);
    when(readSession.execute(any())).thenThrow(RuntimeException.class);

    IStorage topologiesStorage = mock(IStorage.class);
    IStorage buildingsStorage = mock(IStorage.class);
    ViewLayer topologiesDS = new ViewLayer(topologiesStorage, "topologies");
    ViewLayer buildingsDS = new ViewLayer(buildingsStorage, "buildings");

    List<XyzFeatureCodec> results = sampleXyzResponse(3);
    when(topologiesStorage.newReadSession(nc, false)).thenReturn(new MockReadSession(results));
    when(buildingsStorage.newReadSession(nc, false)).thenReturn(readSession);

    ViewLayerCollection viewLayerCollection = new ViewLayerCollection("myCollection", topologiesDS, buildingsDS);
    View view = new View(viewLayerCollection);

    // expect
    assertThrows(UncheckedException.class, () -> view.newReadSession(nc, false).execute(new ReadFeatures()));
  }

  @Test
  void shouldNotQueryForMissingIfOriginalRequestWasOnlyById() throws NoCursor {
    // given
    IStorage topologiesStorage_1 = mock(IStorage.class);
    IStorage topologiesStorage_2 = mock(IStorage.class);
    IReadSession readSession = mock(IReadSession.class);
    when(readSession.execute(any())).thenReturn(new MockResult<>(emptyList()));

    ViewLayer topologiesDS_1 = new ViewLayer(topologiesStorage_1, TOPO);
    ViewLayer topologiesDS_2 = new ViewLayer(topologiesStorage_2, TOPO);

    List<XyzFeatureCodec> results = sampleXyzResponse(3);
    when(topologiesStorage_1.newReadSession(nc, false)).thenReturn(readSession);
    when(topologiesStorage_2.newReadSession(nc, false)).thenReturn(new MockReadSession(results));

    ViewLayerCollection viewLayerCollection = new ViewLayerCollection("myCollection", topologiesDS_1, topologiesDS_2);
    View view = new View(viewLayerCollection);

    // when only by id
    ReadFeatures request1 = RequestHelper.readFeaturesByIdsRequest(TOPO, List.of("1"));
    try (ForwardCursor<XyzFeature, XyzFeatureCodec> cursor = view.newReadSession(nc, false).execute(request1).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
    }
    // then
    verify(readSession, times(1)).execute(any());

    // when not only by id
    clearInvocations(readSession);
    ReadFeatures request2 = new ReadFeatures();
    request2.setPropertyOp(or(eq(id(), 1), eq(app_id(), "app")));
    try (ForwardCursor<XyzFeature, XyzFeatureCodec> cursor = view.newReadSession(nc, false).execute(request2).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
    }
    verify(readSession, times(2)).execute(any());
  }

  @Test
  void testTimeoutExceptionInOneOfTheThreads() {
    IStorage topologiesStorage = mock(IStorage.class);
    IStorage buildingsStorage = mock(IStorage.class);
    ViewLayer topologiesDS = new ViewLayer(topologiesStorage, "topologies");
    ViewLayer buildingsDS = new ViewLayer(buildingsStorage, "buildings");

    // given
    IReadSession topoReadSession = mock(IReadSession.class);
    IReadSession buildReadSession = mock(IReadSession.class);

    when(topoReadSession.execute(any())).thenThrow(new RuntimeException(new TimeoutException()));
    when(buildReadSession.execute(any())).thenReturn(new MockResult<>(sampleXyzResponse(1)));

    when(topologiesStorage.newReadSession(nc, false)).thenReturn(buildReadSession);
    when(buildingsStorage.newReadSession(nc, false)).thenReturn(topoReadSession);

    ViewLayerCollection viewLayerCollection = new ViewLayerCollection("myCollection", topologiesDS, buildingsDS);
    View view = new View(viewLayerCollection);

    Throwable exception = assertThrows(UncheckedException.class, () -> view.newReadSession(nc, false).execute(new ReadFeatures()));
    assertTrue(exception.getMessage().contains("TimeoutException"));
    verify(topoReadSession, times(1)).execute(any());
    verify(buildReadSession, times(1)).execute(any());
  }

  @Test
  void shouldThrowTooManyTasksException() {
    IStorage mockStorage = mock(IStorage.class);
    IRequestLimitManager requestLimitManager= new DefaultRequestLimitManager(30,100);
    AbstractTask.setConcurrencyLimitManager(requestLimitManager);
    long limit = requestLimitManager.getInstanceLevelLimit();
    ViewLayer[] layerDS = new ViewLayer[(int) (limit + 10)];
    //Create ThreadFactory Limit + 10 layers
    for (int ind = 0; ind < layerDS.length; ind++) {
      layerDS[ind] = new ViewLayer(mockStorage, "collection" + ind);
    }
    ViewLayerCollection viewLayerCollection = new ViewLayerCollection("myCollection", layerDS);
    View view = new View(viewLayerCollection);

    List<SimpleTask> tasks = new ArrayList<>();
    try (MockedConstruction<ParallelQueryExecutor> queryExecutor = mockConstruction(ParallelQueryExecutor.class, (mock, context) -> {
      when(mock.queryInParallel(any(), any())).thenAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          List<LayerReadRequest> requests = invocation.getArgument(0);
          for (LayerReadRequest layerReadRequest : requests) {
            SimpleTask singleTask = new SimpleTask<>(null,nc);
            tasks.add(singleTask);
            singleTask.start(() -> {
              Thread.sleep(1000);
              return null;
            });
          }
          return Collections.emptyMap();
        }
      });
    })) {

      ViewReadSession viewReadSession = view.newReadSession(nc, false);
      Throwable ex = assertThrows(TooManyTasks.class, () -> viewReadSession.execute(new ReadFeatures()));
      assertTrue(ex.getMessage().contains("Maximum number of concurrent tasks"));
    }
    //Interrupt sleeping threads in this group to end.
    Optional<SimpleTask> activeTask = tasks.stream().filter(thread -> thread.getThread() != null).findFirst();
    if (activeTask.isPresent()) {
      ThreadGroup threadGroup = activeTask.get().getThread().getThreadGroup();
      threadGroup.interrupt();
      assertEquals(limit, threadGroup.activeCount());
    }
  }

}
