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

import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IterateFeatures extends SearchForFeatures<IterateFeaturesEvent> {

  private long limit;
  private long start;

  public IterateFeatures(IterateFeaturesEvent event, DatabaseHandler dbHandler) throws SQLException {
    super(event, dbHandler);
    limit = event.getLimit();
  }

  @Override
  protected SQLQuery buildQuery(IterateFeaturesEvent event) throws SQLException {
    SQLQuery query = super.buildQuery(event);

    boolean hasHandle = event.getHandle() != null;
    start = hasHandle ? Long.parseLong(event.getHandle()) : 0L;

    if (hasSearch) {
      if (hasHandle)
        query.setQueryFragment("offset", "OFFSET #{startOffset}");
    }
    else {
      if (hasHandle)
        query.setQueryFragment("filterWhereClause", query.getQueryFragment("filterWhereClause") + " AND i > #{startOffset}");

      query.setQueryFragment("orderBy", "ORDER BY i");
    }

    if (hasHandle)
      query.setNamedParameter("startOffset", start);

    return query;
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    FeatureCollection fc = super.handle(rs);
    if (hasSearch && fc.getHandle() != null) {
      fc.setHandle("" + (start + limit)); //Kept for backwards compatibility for now
      fc.setNextPageToken("" + (start + limit));
    }
    return fc;
  }
}
