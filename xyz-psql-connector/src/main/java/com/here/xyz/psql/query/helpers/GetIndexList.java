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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.HEAD_TABLE_SUFFIX;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class GetIndexList extends QueryRunner<String, List<String>> {
  private static final Integer BIG_SPACE_THRESHOLD = 10000;
  private static final Map<String, IndexList> cachedIndices = new HashMap<>();
  private String tableName;
  private List<String> baseTables;
  private String cacheKey;

  /**
   * @param tableName The table of the space
   * @param baseTables The tables of the spaces the space extends (empty if the space is not a composite space)
   */
  public GetIndexList(String tableName, List<String> baseTables) throws SQLException, ErrorResponseException {
    super(tableName);
    this.tableName = tableName;
    this.baseTables = baseTables;
    //The base tables are part of the key, because they contribute to the row count the threshold gets checked against
    cacheKey = String.join(",", Stream.concat(Stream.of(tableName), baseTables.stream()).toList());
  }

  @Override
  public List<String> run(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {

    try { getDataSourceProvider(); }
    catch (NullPointerException e)
    { setDataSourceProvider(dataSourceProvider);  }  // this.dataSourceProvider must not be null in order to use "getSchema()" in buildQuery()

    IndexList indexList = cachedIndices.get(cacheKey);
    if (indexList != null && indexList.expiry >= System.currentTimeMillis())
      return indexList.indices;
    return super.run(dataSourceProvider);
  }

  @Override
  protected SQLQuery buildQuery(String tableName) throws SQLException, ErrorResponseException {
    return new SQLQuery(
      """
        with indata as
        ( select idx_available from
         (
          select jsonb_build_object ( 'src',src,'property', idx_property ) as idx_available
          from xyz_index_list_all_available( #{schema},#{table} )
          where true and
          #{threshold} <=
          ( select sum( coalesce( c.reltuples::bigint, 0 ) )
            from unnest( #{headTables}::text[] ) t(n)
            join pg_class c on c.oid = to_regclass( format( '%I.%I', #{schema}::text, t.n ) )
            where c.relkind = 'r'
          )
         ) o
        ),
        iindata as ( select json_agg( idx_available )::jsonb as idx_available from indata )
        select idx_available from iindata where 0 < ( select count(*) from indata )
      """
        )
        .withNamedParameter(TABLE, tableName)
        .withNamedParameter(SCHEMA, getSchema())
        .withNamedParameter("headTables", Stream.concat(Stream.of(tableName), baseTables.stream())
            .map(table -> table + HEAD_TABLE_SUFFIX).toArray(String[]::new))
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
    cachedIndices.put(cacheKey, indexList);

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
