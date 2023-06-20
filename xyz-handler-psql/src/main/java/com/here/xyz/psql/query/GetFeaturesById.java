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


import com.here.naksha.lib.psql.sql.SQLQuery;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.psql.PsqlHandler;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

public class GetFeaturesById extends GetFeatures<GetFeaturesByIdEvent> {

    public GetFeaturesById(@NotNull GetFeaturesByIdEvent event, @NotNull PsqlHandler psqlConnector)
            throws SQLException {
        super(event, psqlConnector);
    }

    @Override
    protected @NotNull SQLQuery buildQuery(@NotNull GetFeaturesByIdEvent event) throws SQLException {
        final String[] idArray = event.getIds().toArray(new String[0]);
        final String filterWhereClause = "jsondata->>'id' = ANY(#{ids})";
        SQLQuery query = super.buildQuery(event);
        query.setQueryFragment("filterWhereClause", filterWhereClause);
        query.setNamedParameter("ids", idArray);
        return query;
    }
}
