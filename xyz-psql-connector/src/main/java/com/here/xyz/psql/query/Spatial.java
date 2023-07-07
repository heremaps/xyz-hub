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

package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.SpatialQueryEvent;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.XyzResponse;
import java.sql.SQLException;

public abstract class Spatial<E extends SpatialQueryEvent, R extends XyzResponse> extends SearchForFeatures<E, R> {

  public Spatial(E event) throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    SQLQuery geoFilter = buildGeoFilter(event);

    SQLQuery query = super.buildQuery(event);
    SQLQuery geoQuery = new SQLQuery("ST_Intersects(geo, ${{geoFilter}})")
        .withQueryFragment("geoFilter", geoFilter);

    SQLQuery filterWhereClause = new SQLQuery("${{geoQuery}} AND ${{searchQuery}}")
        .withQueryFragment("geoQuery", geoQuery)
        //Use the existing clause as searchQuery (from the base query)
        .withQueryFragment("searchQuery", query.getQueryFragment("filterWhereClause"));

    query
        .withQueryFragment("filterWhereClause", filterWhereClause)
        //Override the geo fragment by a clipped version
        .withQueryFragment("geo", buildClippedGeoFragment(event, geoFilter));

    return query;
  }

  protected abstract SQLQuery buildGeoFilter(E event);

  /**
   * Returns a geo-fragment, which will return the geometry objects clipped by the provided geoFilter
   * depending on whether clipping is active or not.
   */
  protected abstract SQLQuery buildClippedGeoFragment(final E event, SQLQuery geoFilter);
}
