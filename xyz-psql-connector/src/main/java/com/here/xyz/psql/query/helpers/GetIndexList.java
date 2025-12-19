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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.psql.query.helpers.GetIndexList.IndexList;
import static  com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GetIndexList extends QueryRunner<String, IndexList> {
  public static final Integer BIG_SPACE_THRESHOLD = 10000;
  private static final Map<String, IndexList> cachedIndices = new HashMap<>();
  private String tableName;

  public GetIndexList(String tableName) throws SQLException, ErrorResponseException {
    super(tableName);
    this.tableName = tableName;
  }

  @Override
  public IndexList run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {

    try { getDataSourceProvider(); }
    catch (NullPointerException e)
    { setDataSourceProvider(dataSourceProvider);  }  // this.dataSourceProvider must not be null in order to use "getSchema()" in buildQuery()

    IndexList indexList = cachedIndices.get(tableName);
    if (indexList != null && indexList.expiry >= System.currentTimeMillis())
      return indexList;
    return super.run(dataSourceProvider);
  }

  //TODO: take context into account!
  @Override
  protected SQLQuery buildQuery(String tableName) throws SQLException, ErrorResponseException {
    return new SQLQuery(
      """
        WITH cnt AS (
            SELECT
                SUM(COALESCE(reltuples::bigint, 0)) AS count
            FROM pg_class
            WHERE relkind = 'r'
              AND relname IN (
                SELECT unnest(
                           array_remove(
                               array[
                                   m.h_id || '_head',
                                   m.meta#>>'{extends,intermediateTable}' || '_head',
                                   m.meta#>>'{extends,extendedTable}'   || '_head'
                               ],
                               NULL
                           )
                       )
                FROM xyz_config.space_meta m
                WHERE m.schem = #{schema}
                  AND m.h_id  = #{table}
              )
        ),
        idxs AS (
            SELECT jsonb_build_object('property', idx_property, 'idx_name', idx_name) AS idx_available
            FROM xyz_index_list_all_available( #{schema},#{table}) WHERE src='m'
        )
        SELECT (SELECT count FROM cnt),
               COALESCE((SELECT jsonb_agg(idx_available) FROM idxs),'[]'::jsonb) as idx_available;
      """)
        .withNamedParameter(TABLE, tableName)
        .withNamedParameter(SCHEMA, getSchema());
  }

  @Override
  public IndexList handle(ResultSet rs) throws SQLException {
    IndexList indexList;
    try {
      if (!rs.next()) {
        indexList =  new IndexList(null,0);
      }
      else {
        List<OnDemandIndex> indices = new ArrayList<>();
        long count = rs.getLong("count");
        String result = rs.getString("idx_available");

        List<Map<String, Object>> raw = XyzSerializable.deserialize(result, new TypeReference<List<Map<String, Object>>>() { });
        for (Map<String, Object> idx : raw) {
          indices.add(new OnDemandIndex().withPropertyPath((String) idx.get("property")).withIndexName((String)idx.get("idx_name")));
        }
        indexList = new IndexList(indices, count);
      }
    }
    catch (Exception e) {
      indexList = new IndexList(null,0);
    }
    cachedIndices.put(tableName, indexList);

    return indexList;
  }

  public static class IndexList {

    /** Cache indexList for 3 Minutes  */
    static long CACHE_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

    IndexList(List<OnDemandIndex> indices, long count) {
      this.indices = indices;
      this.count = count;
      expiry = System.currentTimeMillis() + CACHE_INTERVAL_MS;
    }

    List<OnDemandIndex> indices;
    long count;
    long expiry;

    public List<OnDemandIndex> getIndices() {
      return indices;
    }

    public long getCount() {
      return count;
    }
  }
}
