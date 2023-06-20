/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.psql.query;


import com.here.mapcreator.ext.naksha.sql.SQLQuery;
import com.here.naksha.lib.core.models.payload.events.feature.SpatialQueryEvent;
import com.here.xyz.psql.PsqlHandler;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

public abstract class Spatial<E extends SpatialQueryEvent> extends SearchForFeatures<E> {

    public Spatial(@NotNull E event, @NotNull PsqlHandler psqlConnector) throws SQLException {
        super(event, psqlConnector);
    }

    /**
     * Returns a geo-fragment, which will return the geometry objects clipped by the provided
     * geoFilter depending on whether clipping is active or not.
     */
    protected abstract SQLQuery buildClippedGeoFragment(final E event, SQLQuery geoFilter);
}
