/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.SQLException;

public class GetFeaturesByBBox<E extends GetFeaturesByBBoxEvent> extends SearchForFeatures<E> {

  public GetFeaturesByBBox(E event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(E event) {
    //NOTE: So far this query runner only handles queries regarding extended spaces
    if (isExtendedSpace(event) && event.getContext() == DEFAULT) {
      String filterWhereClause = "ST_Intersects(geo, ST_MakeEnvelope(#{minLon}, #{minLat}, #{maxLon}, #{maxLat}, 4326))";

      SQLQuery searchQuery = buildSearchFragment(event);
      if (hasSearch)
        filterWhereClause += " AND " + searchQuery.text();

      SQLQuery query = buildQuery(event, filterWhereClause);
      if (hasSearch) {
        searchQuery.replaceFragments();
        query.setNamedParameters(searchQuery.getNamedParameters());
      }
      query.setQueryFragment("limit", buildLimitFragment(event.getLimit()));

      //query.setQueryFragment("filterWhereClause", filterWhereClause);
      final BBox bbox = event.getBbox();
      query.setNamedParameter("minLon", bbox.minLon());
      query.setNamedParameter("minLat", bbox.minLat());
      query.setNamedParameter("maxLon", bbox.maxLon());
      query.setNamedParameter("maxLat", bbox.maxLat());
      return query;
    }
    return null;
  }
}
