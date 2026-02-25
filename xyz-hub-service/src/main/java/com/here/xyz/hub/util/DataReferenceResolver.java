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

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.DataReferenceConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.DataReference;
import io.vertx.core.Future;
import org.apache.logging.log4j.Marker;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public final class DataReferenceResolver {

    private final DataReferenceConfigClient references;
    private final SpaceConfigClient spaces;

    public DataReferenceResolver(DataReferenceConfigClient references) {
        this(references, Service.spaceConfigClient);
    }

    DataReferenceResolver(DataReferenceConfigClient references, SpaceConfigClient spaces) {
        this.references = references;
        this.spaces = spaces;
    }

    public Future<Optional<DataReference>> loadEffectiveById(Marker marker, UUID referenceId) {
        return loadById(marker, referenceId, false);
    }

    /**
     * Load by reference id and return:
     *  - includeStale=true  -> the exact stored reference (if present), with stale references
     *  - includeStale=false -> a non-stale effective reference or Optional.empty()
     */
    public Future<Optional<DataReference>> loadById(Marker marker, UUID referenceId, boolean includeStale) {
        return references.load(referenceId)
                .compose(maybeRef -> {
                    if (maybeRef.isEmpty()) {
                        return Future.succeededFuture(Optional.empty());
                    }

                    DataReference ref = maybeRef.get();

                    if (includeStale) {
                        return Future.succeededFuture(Optional.of(ref));
                    }

                    return makeEffective(marker, ref);
                });
    }

    public Future<List<DataReference>> filterStaleForEntity(Marker marker, String entityId, List<DataReference> refs) {
        return filterForEntity(marker, entityId, refs, false);
    }

    public Future<List<DataReference>> filterForEntity(Marker marker, String entityId, List<DataReference> refs, boolean includeStale) {
        if (includeStale) {
            return Future.succeededFuture(refs);
        }

        return resolveAnchorSpace(marker, entityId)
                .map(maybeAnchor -> {
                    if (maybeAnchor.isEmpty()) {
                        return List.of();
                    }

                    long minCreatedAt = maybeAnchor.get().createdAt;
                    return refs.stream()
                            .filter(r -> ts(r.getCreatedAt()) >= minCreatedAt)
                            .toList();
                });
    }

    private Future<Optional<DataReference>> makeEffective(Marker marker, DataReference ref) {
        String entityId = ref.getEntityId();

        return resolveAnchorSpace(marker, entityId)
                .compose(maybeAnchor -> {
                    if (maybeAnchor.isEmpty()) {
                        // No matching row
                        return Future.succeededFuture(Optional.empty());
                    }

                    long spaceCreatedAt = maybeAnchor.get().createdAt;
                    long refCreatedAt = ts(ref.getCreatedAt());

                    if (spaceCreatedAt <= refCreatedAt) {
                        return Future.succeededFuture(Optional.of(ref));
                    }

                    // Stale -> try replacement for the same entityId
                    return references.load(entityId, null, null, null, null, null, null)
                            .map(list -> pickNewestAtOrAfter(list, spaceCreatedAt));
                });
    }

    private static Optional<DataReference> pickNewestAtOrAfter(List<DataReference> candidates, long minCreatedAt) {
        return candidates.stream()
                .filter(r -> ts(r.getCreatedAt()) >= minCreatedAt)
                .max(Comparator.comparingLong(r -> ts(r.getCreatedAt())));
    }

    private Future<Optional<Anchor>> resolveAnchorSpace(Marker marker, String entityId) {
        return spaces.get(marker, entityId)
                .compose(space -> {
                    if (space != null) {
                        return Future.succeededFuture(Optional.of(anchor(entityId, space)));
                    }

                    Optional<String> parentMap = parentMapHrn(entityId);
                    if (parentMap.isEmpty()) {
                        return Future.succeededFuture(Optional.empty());
                    }

                    return spaces.get(marker, parentMap.get())
                            .map(parentSpace -> parentSpace == null
                                    ? Optional.empty()
                                    : Optional.of(anchor(parentMap.get(), parentSpace)));
                });
    }

    private static Anchor anchor(String spaceIdUsed, Space space) {
        return new Anchor(spaceIdUsed, space.getCreatedAt());
    }

    private static Optional<String> parentMapHrn(String entityId) {
        int idx = entityId == null ? -1 : entityId.lastIndexOf(':');
        if (idx <= 0 || idx == entityId.length() - 1) {
            return Optional.empty();
        }

        return Optional.of(entityId.substring(0, idx));
    }

    private static long ts(Long v) {
        return v == null ? 0L : v;
    }

    private record Anchor(String spaceId, long createdAt) {}
}