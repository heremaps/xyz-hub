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
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.query.helpers.versioning.GetNextVersion;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.ResultSet;
import java.sql.SQLException;

public class WriteFeatures extends ExtendedSpace<WriteFeaturesEvent, FeatureCollection> {
  private long version = -1;
  private WriteFeaturesEvent tmpEvent;

  public WriteFeatures(WriteFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
    this.tmpEvent = event;
  }

  @Override
  protected FeatureCollection run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    version = new GetNextVersion<>(tmpEvent).withDataSourceProvider(dataSourceProvider).run();
    tmpEvent = null;
    return super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildQuery(WriteFeaturesEvent event) throws SQLException, ErrorResponseException {
    return SQLQuery.batchOf(event.getModifications().stream()
        .map(modification -> buildModificationQuery(modification, event.getAuthor(), event.isResponseDataExpected()))
        .toList());
  }

  private SQLQuery buildModificationQuery(Modification modification, String author, boolean responseDataExpected) {
    return new SQLQuery("SELECT write_features(#{featureData}, #{author}, #{onExists}, #{onNotExists}, #{onVersionConflict}, "
        + "#{onMergeConflict}, #{isPartial}, #{version}, #{responseDataExpected});")
        .withNamedParameter("featureData", modification.getFeatureData())
        .withNamedParameter("author", author)
        .withNamedParameter("onExists", modification.getUpdateStrategy().onExists())
        .withNamedParameter("onNotExists", modification.getUpdateStrategy().onNotExists())
        .withNamedParameter("onVersionConflict", modification.getUpdateStrategy().onVersionConflict())
        .withNamedParameter("onMergeConflict", modification.getUpdateStrategy().onMergeConflict())
        .withNamedParameter("isPartial", modification.isPartialUpdates())
        .withNamedParameter("version", version)
        .withNamedParameter("responseDataExpected", responseDataExpected);
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    //TODO: Read feature collection from result (see GetFeatures)
    return null;
  }
}
