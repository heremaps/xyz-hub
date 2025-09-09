/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.responses.XyzError.CONFLICT;
import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.NOT_FOUND;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.PARTITION_SIZE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzError;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import com.here.xyz.util.db.pg.SQLError;
import com.here.xyz.util.runtime.FunctionRuntime;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WriteFeatures extends ExtendedSpace<WriteFeaturesEvent, FeatureCollection> {
  boolean responseDataExpected;

  public WriteFeatures(WriteFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
    responseDataExpected = event.isResponseDataExpected();
  }

  @Override
  protected SQLQuery buildQuery(WriteFeaturesEvent event) throws ErrorResponseException {
    List<String> tables = new ArrayList<>();
    SpaceContext spaceContext = null;

    if (isExtendedSpace(event)) {
      tables.add(getExtendedTable(event));
      if (is2LevelExtendedSpace(event))
        tables.add(getIntermediateTable(event));
      spaceContext = event.getContext();
    }
    tables.add(getDefaultTable(event));

    Map<String, Object> queryContext = new FeatureWriterQueryContextBuilder()
        .withSchema(getSchema())
        .withTables(tables)
        .withSpaceContext(spaceContext)
        .withHistoryEnabled(event.getVersionsToKeep() > 1)
        .withBatchMode(true)
        .with("debug", "true".equals(System.getenv("FW_DEBUG")))
        .with("queryId", FunctionRuntime.getInstance().getStreamId())
        .with("PARTITION_SIZE", PARTITION_SIZE)
        .with("minVersion", event.getMinVersion())
        .with("versionsToKeep", event.getVersionsToKeep())
        .with("pw", getDataSourceProvider().getDatabaseSettings().getPassword())
        .build();

    return new SQLQuery("SELECT write_features(#{modifications}, 'Modifications', #{author}, #{responseDataExpected});")
        .withLoggingEnabled(false)
        .withContext(queryContext)
        .withNamedParameter("modifications", XyzSerializable.serialize(event.getModifications()))
        .withNamedParameter("author", event.getAuthor())
        .withNamedParameter("responseDataExpected", event.isResponseDataExpected());
  }

  @Override
  protected FeatureCollection run(DataSourceProvider dataSourceProvider) throws ErrorResponseException {
    try {
      return super.run(dataSourceProvider);
    }
    catch (SQLException e) {
      final String message = e.getMessage();
      String cleanMessage = message.contains("\n") ? message.substring(0, message.indexOf("\n")) : message;
      throw switch (SQLError.fromErrorCode(e.getSQLState())) {
        case FEATURE_EXISTS, VERSION_CONFLICT_ERROR, MERGE_CONFLICT_ERROR -> new ErrorResponseException(CONFLICT, cleanMessage, e);
        case FEATURE_NOT_EXISTS -> new ErrorResponseException(NOT_FOUND, cleanMessage, e);
        case ILLEGAL_ARGUMENT -> new ErrorResponseException(XyzError.ILLEGAL_ARGUMENT, cleanMessage, e);
        case XYZ_EXCEPTION, UNKNOWN -> new ErrorResponseException(EXCEPTION, e.getMessage(), e);
      };
    }
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    try {
      return responseDataExpected && rs.next()
          ? XyzSerializable.deserialize(rs.getString(1), FeatureCollection.class)
          : new FeatureCollection();
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing query result.", e);
    }
  }
}
