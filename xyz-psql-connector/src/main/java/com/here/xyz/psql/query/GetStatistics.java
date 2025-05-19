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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetStatistics extends ExtendedSpace<GetStatisticsEvent, StatisticsResponse>  {
  private boolean fastMode;
  private String spaceId;
  private Map<String, Object> connectorParams;
  private static final Pattern BBOX_PATTERN = Pattern.compile("^BOX\\(([-\\d\\.]*)\\s([-\\d\\.]*),([-\\d\\.]*)\\s([-\\d\\.]*)\\)$");

  public GetStatistics(GetStatisticsEvent event) throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);
    spaceId = event.getSpace();
    connectorParams = event.getConnectorParams();
    fastMode = event.isFastMode();
  }

  @Override
  protected SQLQuery buildQuery(GetStatisticsEvent event) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT * FROM ${schema}.xyz_statistic_space(#{schema}, #{table}, #{isExtension} )")
        .withVariable(SCHEMA, getSchema())
        .withNamedParameter(SCHEMA, getSchema())
        .withNamedParameter(TABLE, getDefaultTable(event))
        .withNamedParameter("isExtension", event.getContext() == SpaceContext.EXTENSION);
  }

  @Override
  public StatisticsResponse run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    if(fastMode)
      return super.run(dataSourceProvider);

    //TODO: Do version related queries directly inside xyz_statistic_space() and remove GetChangesetStatistics / GetChangesetStatisticsEvent / ChangesetStatisticsResponse ...
    long minVersion;
    long maxVersion;
    Value<Long> minTagVersion;
    try {
      final GetChangesetStatisticsEvent event = new GetChangesetStatisticsEvent()
          .withSpace(spaceId)
          .withConnectorParams(connectorParams);
      ChangesetsStatisticsResponse versionResponse = new GetChangesetStatistics(event).withDataSourceProvider(dataSourceProvider).run();
      minVersion = versionResponse.getMinVersion();
      maxVersion = versionResponse.getMaxVersion();
      minTagVersion = versionResponse.getMinTagVersion() == null ? null : new Value<>(versionResponse.getMinTagVersion());
    }
    catch (ErrorResponseException e) {
      throw new RuntimeException(e);
    }

    return super.run(dataSourceProvider)
        .withMinVersion(new Value<>(minVersion).withEstimated(false))
        .withMaxVersion(new Value<>(maxVersion).withEstimated(false))
        .withMinTagVersion(minTagVersion);
  }

  @Override
  public StatisticsResponse handle(ResultSet rs) throws SQLException {
    try {
      rs.next();

      StatisticsResponse.Value<Long> tablesize = XyzSerializable.deserialize(rs.getString("tablesize"), new TypeReference<Value<Long>>() {});
      StatisticsResponse.Value<List<String>> geometryTypes = XyzSerializable
          .deserialize(rs.getString("geometryTypes"), new TypeReference<StatisticsResponse.Value<List<String>>>() {
          });
      StatisticsResponse.Value<List<StatisticsResponse.PropertyStatistics>> tags = XyzSerializable
          .deserialize(rs.getString("tags"), new TypeReference<StatisticsResponse.Value<List<StatisticsResponse.PropertyStatistics>>>() {
          });
      StatisticsResponse.PropertiesStatistics properties = XyzSerializable.deserialize(rs.getString("properties"), StatisticsResponse.PropertiesStatistics.class);
      StatisticsResponse.Value<Long> count = XyzSerializable.deserialize(rs.getString("count"), new TypeReference<StatisticsResponse.Value<Long>>() {});
      Map<String, Object> bboxMap = XyzSerializable.deserialize(rs.getString("bbox"), new TypeReference<Map<String, Object>>() {});

      final String searchable = rs.getString("searchable");
      properties.setSearchable(StatisticsResponse.PropertiesStatistics.Searchable.valueOf(searchable));

      String bboxs = (String) bboxMap.get("value");
      if (bboxs == null) {
        bboxs = "";
      }

      BBox bbox = new BBox();
      Matcher matcher = BBOX_PATTERN.matcher(bboxs);
      if (matcher.matches()) {
        bbox = new BBox(
            Math.max(-180, Math.min(180, Double.parseDouble(matcher.group(1)))),
            Math.max(-90, Math.min(90, Double.parseDouble(matcher.group(2)))),
            Math.max(-180, Math.min(180, Double.parseDouble(matcher.group(3)))),
            Math.max(-90, Math.min(90, Double.parseDouble(matcher.group(4))))
        );
      }

      return new StatisticsResponse()
          .withBBox(new StatisticsResponse.Value<BBox>().withValue(bbox).withEstimated(bboxMap.get("estimated") == Boolean.TRUE))
          .withByteSize(tablesize)
          .withDataSize(tablesize)
          .withCount(count)
          .withGeometryTypes(geometryTypes)
          .withTags(tags)
          .withProperties(properties);
    }
    catch (JsonProcessingException e) {
      throw new SQLException("Error parsing JSON of statistics result.");
    }
  }
}
