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

import static com.here.naksha.lib.core.models.storage.POp.eq;
import static com.here.naksha.lib.core.models.storage.POp.or;
import static com.here.naksha.lib.core.models.storage.PRef.app_id;
import static com.here.naksha.lib.core.models.storage.PRef.id;
import static com.here.naksha.lib.view.Sample.sampleXyzResponse;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.exceptions.UncheckedException;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.MutableCursor;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;

import java.util.List;

import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.view.merge.MergeByStoragePriority;
import com.here.naksha.lib.view.missing.IgnoreMissingResolver;
import org.junit.jupiter.api.Test;

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

  void testWriteApiNotation() throws NoCursor {

    ViewLayer topologiesDS = new ViewLayer(mock(IStorage.class), "topologies");
    ViewLayer buildingsDS = new ViewLayer(mock(IStorage.class), "buildings");
    ViewLayer topologiesCS = new ViewLayer(mock(IStorage.class), "topologies");


    ViewLayerCollection viewLayerCollection = new ViewLayerCollection("myCollection", topologiesDS, buildingsDS, topologiesCS);

    View view = new View(viewLayerCollection);

    // to discuss if same context is valid to use across all storages
    ViewWriteSession writeSession = view.newWriteSession(nc, true);

    final WriteXyzFeatures request = new WriteXyzFeatures("topologies");
    final XyzFeature feature = new XyzFeature("feature_id_1");
    request.add(EWriteOp.CREATE, feature);

    try (MutableCursor<XyzFeature, XyzFeatureCodec> cursor =
             writeSession.execute(request).getXyzMutableCursor()) {
      cursor.next();
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
    try(ForwardCursor<XyzFeature, XyzFeatureCodec> cursor = view.newReadSession(nc, false).execute(request1).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
    }
    // then
    verify(readSession, times(1)).execute(any());

    // when not only by id
    clearInvocations(readSession);
    ReadFeatures request2 = new ReadFeatures();
    request2.setPropertyOp(or(eq(id(), 1), eq(app_id(), "app")));
    try(ForwardCursor<XyzFeature, XyzFeatureCodec> cursor = view.newReadSession(nc, false).execute(request2).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
    }
    verify(readSession, times(2)).execute(any());
  }

}
