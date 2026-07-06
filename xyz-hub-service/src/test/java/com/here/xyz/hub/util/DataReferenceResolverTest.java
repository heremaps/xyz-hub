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

import static org.assertj.core.api.Assertions.assertThat;
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

        assertThat(result).isEmpty();
        verify(references).load(id);
        verifyNoInteractions(spaces);
    }

    @Test
    void loadById_shouldReturnStoredReference_whenOnlyStaleIsTrue() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("entity-id-1", 100L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));

        Optional<DataReference> result = await(resolver.loadById(marker, id, true));

        assertThat(result).containsSame(ref);
        verifyNoInteractions(spaces);
    }

    @Test
    void loadEffectiveById_shouldReturnSameReference_whenAnchorIsMissing() {
        UUID id = UUID.randomUUID();
        DataReference ref = ref("entity-id-1", 100L).withId(id);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(ref)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(null));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertThat(result).containsSame(ref);
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

        assertThat(result).containsSame(candidate2Newest);
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

        assertThat(result).isEmpty();
    }

    @Test
    void loadEffectiveById_shouldReturnReplacementWithSameUniquenessKey_whenReferenceIsStale() {
        UUID id = UUID.randomUUID();
        Space space = spaceWithCreatedAt(500L);

        DataReference stale = ref("entity-id-1", 100L)
                .withId(id)
                .withEndVersion(10)
                .withObjectType("features")
                .withContentType("ct")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference wrongKeyNewer = ref("entity-id-1", 900L)
                .withEndVersion(11)
                .withObjectType("features")
                .withContentType("ct")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference sameKeyReplacement = ref("entity-id-1", 800L)
                .withEndVersion(10)
                .withObjectType("features")
                .withContentType("ct")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(stale)));
        when(spaces.get(marker, "entity-id-1")).thenReturn(Future.succeededFuture(space));
        when(references.load("entity-id-1", null, null, null, null, null, null))
                .thenReturn(Future.succeededFuture(List.of(wrongKeyNewer, sameKeyReplacement)));

        Optional<DataReference> result = await(resolver.loadEffectiveById(marker, id));

        assertThat(result).containsSame(sameKeyReplacement);
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

        assertThat(result).containsExactly(newer);
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

        assertThat(result).containsExactly(stale);
    }

    @Test
    void filterForEntity_shouldReturnNewest_whenAnchorIsMissing_andOnlyStaleIsFalse() {
        String entityId = "entity-id-1";
        DataReference older = ref(entityId, 100L);
        DataReference newer = ref(entityId, 200L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(older, newer), false));

        assertThat(result).containsExactly(newer);
    }

    @Test
    void filterForEntity_shouldReturnEmpty_whenAnchorIsMissing_andOnlyStaleIsTrue() {
        String entityId = "entity-id-1";
        DataReference r1 = ref(entityId, 100L);
        DataReference r2 = ref(entityId, 200L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(r1, r2), true));

        assertThat(result).isEmpty();
    }

    @Test
    void filterStaleForEntity_shouldReturnOnlyReferencesOlderThanAnchor() {
        String entityId = "entity-id-1";
        Space space = spaceWithCreatedAt(150L);

        DataReference stale = ref(entityId, 100L)
                .withStartVersion(0)
                .withEndVersion(1)
                .withObjectType("features")
                .withContentType("ct-a")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference atAnchor = ref(entityId, 150L)
                .withStartVersion(1)
                .withEndVersion(2)
                .withObjectType("features")
                .withContentType("ct-b")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference fresh = ref(entityId, 151L)
                .withStartVersion(2)
                .withEndVersion(3)
                .withObjectType("features")
                .withContentType("ct-c")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference nullCreatedAt = ref(entityId, null)
                .withStartVersion(3)
                .withEndVersion(4)
                .withObjectType("features")
                .withContentType("ct-d")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(space));

        List<DataReference> result = await(resolver.filterStaleForEntity(marker, entityId, List.of(stale, atAnchor, fresh, nullCreatedAt)));

        assertThat(result).containsExactlyInAnyOrder(stale, nullCreatedAt);
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

        assertThat(result).containsExactly(stale);
    }

    @Test
    void filterForEntity_shouldNotTrimPlainMapHrnToNamespace_whenDirectSpaceIsMissing() {
        String entityId = "hrn:here:data::olp-here:someMap";
        String wrongParent = "hrn:here:data::olp-here";

        DataReference older = ref(entityId, 100L);
        DataReference newer = ref(entityId, 200L);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(older, newer), false));

        assertThat(result).containsExactly(newer);
        verify(spaces).get(marker, entityId);
        verify(spaces, never()).get(marker, wrongParent);
    }

    @Test
    void filterForEntity_shouldReturnDistinctNewestPerUniquenessKey_whenOnlyStaleIsFalse() {
        String entityId = "entity-id-1";
        Space space = spaceWithCreatedAt(150L);

        DataReference keyAOld = ref(entityId, 180L)
                .withEndVersion(10)
                .withObjectType("features")
                .withContentType("ct-a")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference keyANew = ref(entityId, 220L)
                .withEndVersion(10)
                .withObjectType("features")
                .withContentType("ct-a")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference keyB = ref(entityId, 200L)
                .withEndVersion(11)
                .withObjectType("features")
                .withContentType("ct-b")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(space));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(keyAOld, keyANew, keyB), false));

        assertThat(result).containsExactlyInAnyOrder(keyANew, keyB);
    }

    @Test
    void filterForEntity_shouldReturnDistinctNewestPerUniquenessKey_whenAnchorIsMissing_andOnlyStaleIsFalse() {
        String entityId = "entity-id-1";

        DataReference keyAOld = ref(entityId, 100L)
                .withEndVersion(10)
                .withObjectType("features")
                .withContentType("ct-a")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference keyANew = ref(entityId, 200L)
                .withEndVersion(10)
                .withObjectType("features")
                .withContentType("ct-a")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        DataReference keyB = ref(entityId, 150L)
                .withEndVersion(20)
                .withObjectType("features")
                .withContentType("ct-b")
                .withSourceSystem("src")
                .withTargetSystem("tgt");

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(keyAOld, keyANew, keyB), false));

        assertThat(result).containsExactlyInAnyOrder(keyANew, keyB);
    }

    @Test
    void loadById_shouldReturnEmpty_whenReferenceIsExpired() {
        UUID id = UUID.randomUUID();
        long expiredKeepUntil = System.currentTimeMillis() - 1000;
        DataReference expired = ref("entity-id-1", 100L).withId(id).withKeepUntil(expiredKeepUntil);

        when(references.load(id)).thenReturn(Future.succeededFuture(Optional.of(expired)));

        Optional<DataReference> result = await(resolver.loadById(marker, id, false));

        assertThat(result).isEmpty();
        verifyNoInteractions(spaces);
    }

    @Test
    void filterForEntity_shouldExcludeExpiredReferences() {
        String entityId = "entity-id-1";
        long expiredKeepUntil = System.currentTimeMillis() - 1000;
        long futureKeepUntil = System.currentTimeMillis() + 60_000;

        DataReference expired = ref(entityId, 100L).withKeepUntil(expiredKeepUntil);
        DataReference valid = ref(entityId, 200L).withKeepUntil(futureKeepUntil);

        when(spaces.get(marker, entityId)).thenReturn(Future.succeededFuture(null));

        List<DataReference> result = await(resolver.filterForEntity(marker, entityId, List.of(expired, valid), false));

        assertThat(result).containsExactly(valid);
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