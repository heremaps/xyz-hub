/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.Value;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetStatistics extends XyzQueryRunner<GetStatisticsEvent, StatisticsResponse> {

  private static final Pattern BBOX_PATTERN = Pattern.compile("^BOX\\(([-\\d\\.]*)\\s([-\\d\\.]*),([-\\d\\.]*)\\s([-\\d\\.]*)\\)$");

  public GetStatistics(GetStatisticsEvent event) throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);
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
