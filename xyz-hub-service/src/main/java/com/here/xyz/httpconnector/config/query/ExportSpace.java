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

package com.here.xyz.httpconnector.config.query;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;

//TODO: Remove that hack after refactoring is complete
public interface ExportSpace<E extends SearchForFeaturesEvent> {
  SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException;

  void setDataSourceProvider(DataSourceProvider dataSourceProvider);

  ExportSpace<E> withSelectionOverride(SQLQuery selectionOverride);

  ExportSpace<E> withGeoOverride(SQLQuery geoOverride);

  ExportSpace<E> withCustomWhereClause(SQLQuery whereClauseOverride);

  default SQLQuery patchSelectClause(SQLQuery selectClause, SQLQuery selectionOverride) {
    if (selectionOverride != null)
      return selectClause.withQueryFragment("selection", selectionOverride);
    return selectClause;
  }

  default SQLQuery patchWhereClause(SQLQuery filterWhereClause, SQLQuery customWhereClause) {
    if (customWhereClause == null)
      return filterWhereClause;
    SQLQuery customizedWhereClause = new SQLQuery("${{innerFilterWhereClause}} ${{customWhereClause}}")
        .withQueryFragment("innerFilterWhereClause", filterWhereClause)
        .withQueryFragment("customWhereClause", customWhereClause);
    return customizedWhereClause;
  }

  default boolean isVersionRange(E event) {
    return event.getRef().isRange();
  }

  default SQLQuery buildVersionComparisonTileCalculation(SelectiveEvent event) {
    Ref ref = event.getRef();

    if( ref == null || !ref.isRange() )
      return new SQLQuery("");

    return new SQLQuery(  // e.g. all features that where visible either in version "fromVersion" or "toVersion" and have changed between fromVersion and toVersion
        """
         AND (    ( version <= #{toVersion} and next_version > #{toVersion} )
               OR ( version <= #{fromVersion} and next_version > #{fromVersion} )
             )
         AND id in ( select distinct id FROM ${schema}.${table} WHERE version > #{fromVersion} and version <= #{toVersion} )
        """
    ).withNamedParameter("fromVersion", ref.getStart().getVersion())
        .withNamedParameter("toVersion", ref.getEnd().getVersion());
  }

  SQLQuery buildSelectClause(E event, int dataset);
  SQLQuery buildFiltersFragment(E event, boolean isExtension, SQLQuery filterWhereClause, int dataset);
  SQLQuery buildFilterWhereClause(E event);
  String getSchema();
  String getDefaultTable(E event);
  String buildOuterOrderByFragment(ContextAwareEvent event);
  SQLQuery buildLimitFragment(E event);

  default SQLQuery buildVersionCheckFragment(E event) {
    return new SQLQuery("${{versionComparison}} ${{nextVersion}} ${{minVersion}}")
        .withQueryFragment("versionComparison", buildVersionComparisonTileCalculation(event))
        .withQueryFragment("nextVersion", new SQLQuery("")) // remove standard fragment s. buildVersionComparisonTileCalculation
        .withQueryFragment("minVersion", new SQLQuery("")); // remove standard fragment
  }

  default SQLQuery buildMainIncrementalQuery(E event) {
    return new SQLQuery(
        """ 
         SELECT ${{selectClause}} FROM ${schema}.${table} 
         WHERE ${{filters}} ${{versionCheck}} ${{outerOrderBy}} ${{limit}}
        """
    )
        .withQueryFragment("selectClause", buildSelectClause(event, 0))
        .withQueryFragment("filters", buildFiltersFragment(event, false, buildFilterWhereClause(event), 0))
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, getDefaultTable(event))
        .withQueryFragment("versionCheck", buildVersionCheckFragment(event))
        .withQueryFragment("outerOrderBy", buildOuterOrderByFragment(event))
        .withQueryFragment("limit", buildLimitFragment(event));
  }
}
