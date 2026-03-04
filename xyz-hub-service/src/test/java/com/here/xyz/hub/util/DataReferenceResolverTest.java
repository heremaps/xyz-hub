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

package com.here.xyz.hub.util;

import com.here.xyz.hub.config.DataReferenceConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.DataReference;
import io.vertx.core.Future;
import org.apache.logging.log4j.Marker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataReferenceResolverTest {

    @Mock
    private DataReferenceConfigClient references;
    @Mock
    private SpaceConfigClient spaces;
    @Mock
    private Marker marker;

    private DataReferenceResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new DataReferenceResolver(references, spaces);
    }

    @Test
    void loadById_shouldReturnEmpty_whenReferenceDoesNotExist() {
        UUID id = UUID.randomUUID();
        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.empty()));

        Optional<DataReference> result = await(resolver.loadById(marker, id, false));

        assertTrue(result.isEmpty());
        verify(references).load(id);
        verifyNoInteractions(spaces);
    }

    @Test
    void loadById_shouldReturnStoredReference_whenOnlyStaleIsTrue() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("entity-id-1", 100L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));

        Optional<DataReference> result = await(resolver.loadById(marker, id, true));

        assertTrue(result.isPresent());
        assertSame(ref, result.get());
        verifyNoInteractions(spaces);
    }

    @Test
    void loadEffectiveById_shouldReturnSameReference_whenAnchorIsMissing() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("entity-id-1", 100L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(null));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertTrue(result.isPresent());
        assertSame(ref, result.get());
    }

    @Test
    void loadEffectiveById_shouldReturnReplacement_whenReferenceIsStale() {
        UUID id = UUID.randomUUID();
        Space space = spaceWithCreatedAt(500L);

        DataReference stale = ref("entity-id-1", 100L).withId(id);
        DataReference candidate1 = ref("entity-id-1", 500L);
        DataReference candidate2Newest = ref("entity-id-1", 800L);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(stale)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(space));
        when(references.load("entity-id-1", null, null, null, null, null, null))
                .thenReturn(Future.succeededFuture(List.of(candidate1, candidate2Newest)));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertTrue(result.isPresent());
        assertSame(candidate2Newest, result.get());
    }

    @Test
    void loadEffectiveById_shouldReturnEmpty_whenReferenceIsStaleAndNoReplacementExists() {
        UUID id = UUID.randomUUID();
        Space space = spaceWithCreatedAt(500L);

        DataReference stale = ref("entity-id-1", 100L).withId(id);
        DataReference tooOld = ref("entity-id-1", 400L);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(stale)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(space));
        when(references.load("entity-id-1", null, null, null, null, null, null))
                .thenReturn(Future.succeededFuture(List.of(tooOld)));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertTrue(result.isEmpty());
    }

    @Test
    void filterForEntity_shouldReturnNewestNonStale_whenOnlyStaleIsFalse() {
        String entityId = "entity-id-1";
        Space space = spaceWithCreatedAt(150L);

        DataReference stale = ref(entityId, 100L);
        DataReference latest = ref(entityId, 150L);
        DataReference newer = ref(entityId, 180L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(space));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(stale, latest, newer), false));

        assertEquals(List.of(newer), result);
    }

    @Test
    void filterForEntity_shouldReturnOnlyStale_whenOnlyStaleIsTrue() {
        String entityId = "entity-id-1";
        Space space = spaceWithCreatedAt(150L);

        DataReference stale = ref(entityId, 100L);
        DataReference latest = ref(entityId, 150L);
        DataReference newer = ref(entityId, 180L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(space));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(stale, latest, newer), true));

        assertEquals(List.of(stale), result);
    }

    @Test
    void filterForEntity_shouldReturnNewest_whenAnchorIsMissing_andOnlyStaleIsFalse() {
        String entityId = "entity-id-1";
        DataReference older = ref(entityId, 100L);
        DataReference newer = ref(entityId, 200L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(older, newer), false));

        assertEquals(List.of(newer), result);
    }

    @Test
    void filterForEntity_shouldReturnEmpty_whenAnchorIsMissing_andOnlyStaleIsTrue() {
        String entityId = "entity-id-1";
        DataReference r1 = ref(entityId, 100L);
        DataReference r2 = ref(entityId, 200L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(r1, r2), true));

        assertTrue(result.isEmpty());
    }

    @Test
    void filterStaleForEntity_shouldReturnOnlyReferencesOlderThanAnchor() {
        String entityId = "entity-id-1";
        Space space = spaceWithCreatedAt(150L);

        DataReference stale = ref(entityId, 100L);
        DataReference atAnchor = ref(entityId, 150L);
        DataReference fresh = ref(entityId, 151L);
        DataReference nullCreatedAt = ref(entityId, null);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(space));

        List<DataReference> result = await(resolver.filterStaleForEntity(marker, entityId, List.of(stale, atAnchor, fresh, nullCreatedAt)));

        assertEquals(List.of(stale, nullCreatedAt), result);
    }

    @Test
    void filterStaleForEntity_shouldUseParentMapAnchor_whenDirectSpaceIsMissing() {
        String entityId = "hrn:here:data::olp-here:someMap:layer";
        String parent = "hrn:here:data::olp-here:someMap";

        Space parentSpace = spaceWithCreatedAt(1000L);

        DataReference stale = ref(entityId, 999L);
        DataReference atAnchor = ref(entityId, 1000L);
        DataReference fresh = ref(entityId, 1001L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));
        when(spaces.get(marker, parent)).thenReturn(Future.succeededFuture(parentSpace));

        List<DataReference> result = await(resolver.filterStaleForEntity(marker, entityId, List.of(stale, atAnchor, fresh)));

        assertEquals(List.of(stale), result);
    }

    private static DataReference ref(String entityId, Long createdAt) {
        DataReference r = new DataReference().withEntityId(entityId);
        if (createdAt != null) {
            r.withCreatedAt(createdAt);
        }

        return r;
    }

    private static Space spaceWithCreatedAt(long createdAt) {
        Space s = new Space();
        s.setCreatedAt(createdAt);
        return s;
    }

    private static <T> T await(Future<T> f) {
        return f.toCompletionStage().toCompletableFuture().join();
    }
}