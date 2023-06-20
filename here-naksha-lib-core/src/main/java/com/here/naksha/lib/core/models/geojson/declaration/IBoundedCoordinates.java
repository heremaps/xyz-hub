/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.naksha.lib.core.models.geojson.declaration;


import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import java.util.List;

public interface IBoundedCoordinates {

    static BBox calculate(List<? extends IBoundedCoordinates> coordinates) {
        if (coordinates.size() == 0) {
            return null;
        }

        double minLon = Double.POSITIVE_INFINITY;
        double minLat = Double.POSITIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;

        for (IBoundedCoordinates el : coordinates) {
            BBox bbox = el.calculateBBox();
            if (bbox != null) {
                if (bbox.minLon() < minLon) {
                    minLon = bbox.minLon();
                }
                if (bbox.minLat() < minLat) {
                    minLat = bbox.minLat();
                }
                if (bbox.maxLon() > maxLon) {
                    maxLon = bbox.maxLon();
                }
                if (bbox.maxLat() > maxLat) {
                    maxLat = bbox.maxLat();
                }
            }
        }

        if (minLon != Double.POSITIVE_INFINITY
                && minLat != Double.POSITIVE_INFINITY
                && maxLon != Double.NEGATIVE_INFINITY
                && maxLat != Double.NEGATIVE_INFINITY) {
            return new BBox(minLon, minLat, maxLon, maxLat);
        }

        return null;
    }

    /**
     * Calculates the latitude of the point between lon1/lat1 and lon2/lat2, that has a latitude of
     * 180/0.
     *
     * @param lon1 the negative longitude
     * @param lat1 the latitude at the coordinate with negative longitude
     * @param lon2 the positive longitude
     * @param lat2 the latitude at the coordinate with positive longitude
     */
    @SuppressWarnings({"unused"})
    static double calculateDateBorderLatitude(double lon1, double lat1, double lon2, double lat2) {
        // Distance to date order for longitude 1 and 2 are a0 and a1 of the polynomial.
        double a0 = 180d + lon1;
        double a1 = 180d - lon2;

        // Polynomial calculation of latitude from polynomial lat1*a0 +
        return (lat1 * a1 + lat2 * a0) / (a0 + a1);
    }

    BBox calculateBBox();
}
