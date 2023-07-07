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

package com.here.xyz.psql.query.helpers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.datasource.DataSourceProvider;
import com.here.xyz.psql.query.ModifySpace;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GetIndexList extends QueryRunner<String, List<String>> {
  private static final Integer BIG_SPACE_THRESHOLD = 10000;
  private static final Map<String, IndexList> cachedIndices = new HashMap<>();
  private String tableName;

  public GetIndexList(String tableName) throws SQLException, ErrorResponseException {
    super(tableName);
    this.tableName = tableName;
  }

  @Override
  public List<String> run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    IndexList indexList = cachedIndices.get(tableName);
    if (indexList != null && indexList.expiry >= System.currentTimeMillis())
      return indexList.indices;
    return super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildQuery(String tableName) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT idx_available FROM " + ModifySpace.IDX_STATUS_TABLE_FQN
        + " WHERE spaceid = #{table} AND count >= #{threshold}")
        .withNamedParameter(TABLE, tableName)
        .withNamedParameter("threshold", BIG_SPACE_THRESHOLD);
  }

  @Override
  public List<String> handle(ResultSet rs) throws SQLException {
    IndexList indexList;
    try {
      if (!rs.next()) {
        indexList =  new IndexList(null);
      }
      else {
        List<String> indices = new ArrayList<>();

        String result = rs.getString("idx_available");
        List<Map<String, Object>> raw = XyzSerializable.deserialize(result, new TypeReference<List<Map<String, Object>>>() {
        });
        for (Map<String, Object> one : raw) {
          /*
           * Indices are marked as:
           * a = automatically created (auto-indexing)
           * m = manually created (on-demand)
           * o = sortable - manually created (on-demand) --> first single sortable propertie is always ascending
           * s = basic system indices
           */
          if (one.get("src").equals("a") || one.get("src").equals("m"))
            indices.add((String) one.get("property"));
          else if (one.get("src").equals("o"))
            indices.add("o:" + (String) one.get("property"));
        }
        indexList = new IndexList(indices);
      }
    }
    catch (Exception e) {
      indexList = new IndexList(null);
    }
    cachedIndices.put(tableName, indexList);

    return indexList.indices;
  }

  private static class IndexList {

    /** Cache indexList for 3 Minutes  */
    static long CACHE_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

    IndexList(List<String> indices) {
      this.indices = indices;
      expiry = System.currentTimeMillis() + CACHE_INTERVAL_MS;
    }

    List<String> indices;
    long expiry;
  }
}
