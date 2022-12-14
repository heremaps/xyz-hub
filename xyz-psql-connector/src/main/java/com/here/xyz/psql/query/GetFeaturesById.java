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

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import java.sql.SQLException;

public class GetFeaturesById extends GetFeatures<GetFeaturesByIdEvent> {

  public GetFeaturesById(GetFeaturesByIdEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(GetFeaturesByIdEvent event) throws SQLException {
    String[] idArray = event.getIds().toArray(new String[0]);
    String filterWhereClause = "${{idColumn}} = ANY(#{ids})";

    return super.buildQuery(event)
        .withQueryFragment("filterWhereClause", filterWhereClause)
        .withQueryFragment("idColumn", buildIdFragment(event))
        .withNamedParameter("ids", idArray);
  }
}
