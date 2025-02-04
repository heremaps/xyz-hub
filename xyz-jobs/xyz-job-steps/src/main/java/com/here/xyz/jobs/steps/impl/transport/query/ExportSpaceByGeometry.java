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
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.SelectiveEvent;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.GetFeatures;
import com.here.xyz.psql.query.GetFeaturesByGeometry;
import com.here.xyz.util.db.SQLQuery;

import java.sql.SQLException;

public class ExportSpaceByGeometry 
  extends GetFeaturesByGeometry 
  implements ExportSpace<GetFeaturesByGeometryEvent> {
  SQLQuery selectionOverride;
  SQLQuery geoOverride;
  SQLQuery customWhereClause;

  public ExportSpaceByGeometry(GetFeaturesByGeometryEvent event) throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  public SQLQuery buildQuery(GetFeaturesByGeometryEvent event) throws SQLException, ErrorResponseException {
    return super.buildQuery(event);
  }

  @Override
  protected SQLQuery buildSelectClause(GetFeaturesByGeometryEvent event, int dataset) {
    return patchSelectClause(super.buildSelectClause(event, dataset), selectionOverride);
  }

  @Override
  protected SQLQuery buildGeoFragment(GetFeaturesByGeometryEvent event) {
    if (geoOverride == null)
      return super.buildGeoFragment(event);
    return geoOverride;
  }

  @Override
  protected SQLQuery buildFilterWhereClause(GetFeaturesByGeometryEvent event) {
    return patchWhereClause(super.buildFilterWhereClause(event), customWhereClause);
  }

  @Override
  protected SQLQuery buildLimitFragment(GetFeaturesByGeometryEvent event) {
    return new SQLQuery("");
  }

  @Override
  public ExportSpace<GetFeaturesByGeometryEvent> withSelectionOverride(SQLQuery selectionOverride) {
    this.selectionOverride = selectionOverride;
    return this;
  }

  @Override
  public ExportSpace<GetFeaturesByGeometryEvent> withGeoOverride(SQLQuery geoOverride) {
    this.geoOverride = geoOverride;
    return this;
  }

  @Override
  public ExportSpace<GetFeaturesByGeometryEvent> withCustomWhereClause(SQLQuery customWhereClause) {
    this.customWhereClause = customWhereClause;
    return this;
  }
}
