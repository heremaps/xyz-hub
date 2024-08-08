/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;

public class GetFeaturesById extends GetFeatures<GetFeaturesByIdEvent, FeatureCollection> {

  private boolean emptyRequest;

  public GetFeaturesById(GetFeaturesByIdEvent event) throws SQLException, ErrorResponseException {
    super(event);
    emptyRequest = event.getIds() == null || event.getIds().size() == 0;
  }

  @Override
  protected FeatureCollection run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    return emptyRequest ? new FeatureCollection() : super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildFilterWhereClause(GetFeaturesByIdEvent event) {
    return new SQLQuery("id = ANY(#{ids})")
        .withNamedParameter("ids", event.getIds().toArray(new String[0]));
  }
}
