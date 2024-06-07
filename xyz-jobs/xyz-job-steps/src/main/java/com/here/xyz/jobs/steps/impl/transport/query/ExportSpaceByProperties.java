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
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.query.SearchForFeatures;
import com.here.xyz.util.db.SQLQuery;

import java.sql.SQLException;

public class ExportSpaceByProperties extends SearchForFeatures<SearchForFeaturesEvent, FeatureCollection>
    implements ExportSpace<SearchForFeaturesEvent> {
  SQLQuery selectionOverride;
  SQLQuery geoOverride;
  SQLQuery customWhereClause;

  public ExportSpaceByProperties(GetFeaturesByGeometryEvent event) throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  public SQLQuery buildQuery(SearchForFeaturesEvent event) throws SQLException, ErrorResponseException {
    return super.buildQuery(event);
  }

  @Override
  protected SQLQuery buildSelectClause(SearchForFeaturesEvent event, int dataset) {
    return patchSelectClause(super.buildSelectClause(event, dataset), selectionOverride);
  }

  @Override
  protected SQLQuery buildGeoFragment(SearchForFeaturesEvent event) {
    if (geoOverride == null)
      return super.buildGeoFragment(event);
    return geoOverride;
  }

  @Override
  protected SQLQuery buildFilterWhereClause(SearchForFeaturesEvent event) {
    return patchWhereClause(super.buildFilterWhereClause(event), customWhereClause);
  }

  @Override
  protected SQLQuery buildLimitFragment(SearchForFeaturesEvent event) {
    return new SQLQuery("");
  }

  @Override
  public ExportSpace<SearchForFeaturesEvent> withSelectionOverride(SQLQuery selectionOverride) {
    this.selectionOverride = selectionOverride;
    return this;
  }

  @Override
  public ExportSpace<SearchForFeaturesEvent> withGeoOverride(SQLQuery geoOverride) {
    this.geoOverride = geoOverride;
    return this;
  }

  @Override
  public ExportSpace<SearchForFeaturesEvent> withCustomWhereClause(SQLQuery customWhereClause) {
    this.customWhereClause = customWhereClause;
    return this;
  }
}
