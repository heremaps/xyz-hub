/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.hub.rest.jobs;

import static org.junit.Assert.assertThrows;

import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Point;
import org.junit.Ignore;
import org.junit.jupiter.api.Disabled;

@Ignore("Obsolete / Deprecated")
@Disabled("Obsolete / Deprecated")
public class JobDatasetsIT {

// uncommeted validation s.  https://here-technologies.atlassian.net/browse/DS-657
//    @Test
    public void createInvalidSpatialFilter() {

        assertThrows(InvalidGeometryException.class,
                     () -> new SpatialFilter().withGeometry(new Point().withCoordinates(new PointCoordinates(399,399))));

    }


}
