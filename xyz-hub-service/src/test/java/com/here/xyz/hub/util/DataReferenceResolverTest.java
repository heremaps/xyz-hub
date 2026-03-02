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
    void loadById_shouldReturnStoredReference_whenIncludeStaleTrue_evenIfNoAnchorExists() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("entity-id-1", 100L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));

        Optional<DataReference> result = await(resolver.loadById(marker, id, true));

        assertTrue(result.isPresent());
        assertSame(ref, result.get());

        verify(references).load(id);
        verifyNoInteractions(spaces);
    }

    @Test
    void loadEffectiveById_shouldReturnSameReference_whenNoAnchorExists_andIncludeStaleDefaultFalse() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("entity-id-1", 100L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(null));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id)); // includeStale=false

        assertTrue(result.isPresent());
        assertSame(ref, result.get());

        verify(references).load(id);
        verify(spaces).get(marker, "entity-id-1");
        verifyNoMoreInteractions(references, spaces);
    }

    @Test
    void loadEffectiveById_shouldReturnSameReference_whenEntityIdHasNoParentHrn_andDirectMissing() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("no-colon-parent", 200L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));
        when(spaces.get(marker, "no-colon-parent")).thenReturn(Future.succeededFuture(null));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertTrue(result.isPresent());
        assertSame(ref, result.get());

        verify(references).load(id);
        verify(spaces).get(marker, "no-colon-parent");
        verifyNoMoreInteractions(references, spaces);

        verify(references, never()).load(eq("no-colon-parent"), any(), any(), any(), any(), any(), any());
    }

    @Test
    void filterStaleForEntity_shouldNotFilter_whenNoAnchorExists_andIncludeStaleDefaultFalse() {
        String entityId = "entity-id-1";

        DataReference r1 = ref(entityId, 100L);
        DataReference r2 = ref(entityId, 200L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));

        List<DataReference> result = await(resolver.filterStaleForEntity(marker, entityId, List.of(r1, r2)));

        assertEquals(List.of(r1, r2), result);

        verify(spaces).get(marker, entityId);
        verifyNoMoreInteractions(spaces);
    }

    @Test
    void loadEffectiveById_shouldReturnSameReference_whenAnchorCreatedAtIsOlderOrEqual() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("entity-id-1", 200L).withId(id);

        Space space = spaceWithCreatedAt(150L);
        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(space));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertTrue(result.isPresent());
        assertSame(ref, result.get());

        verify(references).load(id);
        verify(spaces).get(marker, "entity-id-1");
        verifyNoMoreInteractions(references, spaces);
    }

    @Test
    void loadEffectiveById_shouldTryParentHrn_whenDirectSpaceMissing() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("hrn:here:data::olp-here:someMap:layer", 200L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));
        when(spaces.get(marker, "hrn:here:data::olp-here:someMap:layer")).thenReturn(Future.succeededFuture(null));
        when(spaces.get(marker, "hrn:here:data::olp-here:someMap")).thenReturn(Future.succeededFuture(spaceWithCreatedAt(150L)));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertTrue(result.isPresent());
        assertSame(ref, result.get());

        verify(references).load(id);
        verify(spaces).get(marker, "hrn:here:data::olp-here:someMap:layer");
        verify(spaces).get(marker, "hrn:here:data::olp-here:someMap");
        verifyNoMoreInteractions(references, spaces);
    }

    @Test
    void loadEffectiveById_shouldReturnReplacement_whenReferenceIsStaleAndReplacementExists() {
        UUID id = UUID.randomUUID();

        Space space = spaceWithCreatedAt(500L);

        DataReference stale = ref("entity-id-1", 100L).withId(id);
        DataReference oldCandidate = ref("entity-id-1", 200L);
        DataReference goodCandidate1 = ref("entity-id-1", 500L);
        DataReference goodCandidate2Newest = ref("entity-id-1", 800L);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(stale)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(space));
        when(references.load("entity-id-1", null, null, null, null, null, null))
                .thenReturn(Future.succeededFuture(List.of(oldCandidate, goodCandidate1, goodCandidate2Newest)));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertTrue(result.isPresent());
        assertSame(goodCandidate2Newest, result.get());

        verify(references).load(id);
        verify(spaces).get(marker, "entity-id-1");
        verify(references).load("entity-id-1", null, null, null, null, null, null);
        verifyNoMoreInteractions(references, spaces);
    }

    @Test
    void loadEffectiveById_shouldReturnEmpty_whenStaleAndNoReplacementExists() {
        UUID id = UUID.randomUUID();

        Space space = spaceWithCreatedAt(500L);
        DataReference stale = ref("entity-id-1", 100L).withId(id);
        DataReference candidateTooOld = ref("entity-id-1", 400L);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(stale)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(space));
        when(references.load("entity-id-1", null, null, null, null, null, null))
                .thenReturn(Future.succeededFuture(List.of(candidateTooOld)));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertTrue(result.isEmpty(), "Should not return Optional.of(null); return Optional.empty instead");

        verify(references).load(id);
        verify(spaces).get(marker, "entity-id-1");
        verify(references).load("entity-id-1", null, null, null, null, null, null);
        verifyNoMoreInteractions(references, spaces);
    }

    @Test
    void loadById_shouldReturnStaleReference_whenIncludeStaleTrue_evenIfStale() {
        UUID id = UUID.randomUUID();

        Space space = spaceWithCreatedAt(500L);
        DataReference stale = ref("entity-id-1", 100L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(stale)));

        Optional<DataReference> result = await(resolver.loadById(marker, id, true));

        assertTrue(result.isPresent());
        assertSame(stale, result.get());

        verify(references).load(id);
        verifyNoInteractions(spaces);
    }

    @Test
    void filterForEntity_shouldReturnAll_whenIncludeStaleTrue_evenIfAnchorMissing() {
        String entityId = "entity-id-1";
        DataReference r1 = ref(entityId, 100L);
        DataReference r2 = ref(entityId, 200L);

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(r1, r2), true));

        assertEquals(List.of(r1, r2), result);
        verifyNoInteractions(spaces);
    }

    @Test
    void filterStaleForEntity_shouldFilterOutOlderThanAnchorCreatedAt() {
        String entityId = "entity-id-1";

        Space space = spaceWithCreatedAt(150L);

        DataReference tooOld = ref(entityId, 100L);
        DataReference ok1 = ref(entityId, 150L);
        DataReference ok2 = ref(entityId, 151L);
        DataReference nullCreatedAt = ref(entityId, null); // treated as 0

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(space));

        List<DataReference> result = await(resolver.filterStaleForEntity(marker, entityId, List.of(tooOld, ok1, ok2, nullCreatedAt)));

        assertEquals(List.of(ok1, ok2), result);
    }

    @Test
    void filterStaleForEntity_shouldUseParentHrnAnchor_whenDirectSpaceMissing() {
        String entityId = "hrn:here:data::olp-here:someMap:layer";
        String parent = "hrn:here:data::olp-here:someMap";

        Space parentSpace = spaceWithCreatedAt(1000L);

        DataReference r1 = ref(entityId, 999L);
        DataReference r2 = ref(entityId, 1000L);
        DataReference r3 = ref(entityId, 1001L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));
        when(spaces.get(marker, parent)).thenReturn(Future.succeededFuture(parentSpace));

        List<DataReference> result = await(resolver.filterStaleForEntity(marker, entityId, List.of(r1, r2, r3)));

        assertEquals(List.of(r2, r3), result);
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