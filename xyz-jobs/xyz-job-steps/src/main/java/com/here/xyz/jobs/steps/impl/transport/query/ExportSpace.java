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

package com.here.xyz.jobs.steps.impl.transport.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.GetFeatures;
import com.here.xyz.util.db.SQLQuery;

import java.sql.SQLException;

//TODO: Remove that hack after refactoring is complete
public interface ExportSpace<E extends SearchForFeaturesEvent> {
  SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException;

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

/****/
  default SQLQuery buildVersionComparisonForRange(SelectiveEvent event) {
   Ref ref = event.getRef();
   if (event.getVersionsToKeep() == 1 || ref.isAllVersions() || ref.isHead())
     return new SQLQuery("");

   return new SQLQuery("AND version > #{fromVersion} AND version <= #{toVersion}")
      .withNamedParameter("fromVersion", ref.getStartVersion())
      .withNamedParameter("toVersion", ref.getEndVersion());
  }

 default SQLQuery buildNextVersionFragmentForRange(Ref ref, boolean historyEnabled, String versionParamName) {
   if (!historyEnabled || ref.isAllVersions())
     return new SQLQuery("");

   boolean endVersionIsHead = ref.getEndVersion() == GetFeatures.MAX_BIGINT;
   //TODO: review semantic of "NextVersionFragment" in case of ref.isRange
   return new SQLQuery("AND next_version ${{op}} #{" + versionParamName + "}")
      .withQueryFragment("op", endVersionIsHead ? "=" : ">")
      .withNamedParameter(versionParamName, endVersionIsHead ? GetFeatures.MAX_BIGINT : ref.getEndVersion());
 }
/****/
}
