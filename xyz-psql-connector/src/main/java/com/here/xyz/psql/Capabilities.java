/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.psql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Capabilities {

  /** Determines if PropertiesQuery can be executed. Check if required Indices are created. */
  private static List<String> sortableCanSearchForIndex(List<String> indices) {
    if (indices == null) return null;
    List<String> skeys = new ArrayList<String>();
    for (String k : indices)
      if (k.startsWith("o:")) skeys.add(k.replaceFirst("^o:([^,]+).*$", "$1"));

    return skeys;
  }

  public static boolean canSearchFor(@Nullable PropertiesQuery query, @NotNull PsqlEventProcessor connector) {
    if (query == null) {
      return true;
    }

    try {
      List<String> keys =
          query.stream()
              .flatMap(List::stream)
              .filter(k -> k.getKey() != null && k.getKey().length() > 0)
              .map(PropertyQuery::getKey)
              .collect(Collectors.toList());

      int idx_check = 0;

      for (String key : keys) {

        /**
         * properties.foo vs foo (root) If hub receives "f.foo=bar&p.foo=bar" it will generates a
         * PropertyQuery with properties.foo=bar and foo=bar
         */
        boolean isPropertyQuery = key.startsWith("properties.");

        /**
         * If property query hits default system index - allow search. [id,
         * properties.@ns:com:here:xyz.createdAt, properties.@ns:com:here:xyz.updatedAt]"
         */
        if (key.equals("id")
            || key.equals("properties.@ns:com:here:xyz.createdAt")
            || key.equals("properties.@ns:com:here:xyz.updatedAt")) return true;

        /** Check if custom Indices are available. Eg.: properties.foo1&f.foo2 */
        List<String> indices = IndexList.getIndexList(connector);

        /** The table has not many records - Indices are not required */
        if (indices == null) {
          return true;
        }

        List<String> sindices = sortableCanSearchForIndex(indices);
        /**
         * If it is a root property query "foo=bar" we extend the suffix "f." If it is a property
         * query "properties.foo=bar" we remove the suffix "properties."
         */
        String searchKey = isPropertyQuery ? key.substring("properties.".length()) : "f." + key;

        if (indices.contains(searchKey) || (sindices != null && sindices.contains(searchKey))) {
          /** Check if all properties are indexed */
          idx_check++;
        }
      }

      if (idx_check == keys.size()) return true;

      return IndexList.getIndexList(connector) == null;
    } catch (Exception e) {
      // In all cases, when something with the check went wrong, allow the search
      return true;
    }
  }

  public static class IndexList {
    /** Cache indexList for 3 Minutes */
    static long CACHE_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

    /** Get list of indexed Values from a XYZ-Space */
    public static @Nullable List<@NotNull String> getIndexList(@NotNull PsqlEventProcessor psqlConnector)
        throws SQLException {
      IndexList indexList = cachedIndices.get(psqlConnector.spaceId());
      if (indexList != null && indexList.expiry >= System.currentTimeMillis()) {
        return indexList.indices;
      }

      indexList =
          psqlConnector.executeQuery(
              SQLQueryBuilder.generateIDXStatusQuery(
                  psqlConnector.spaceSchema(), psqlConnector.spaceTable()),
              Capabilities::rsHandler);

      cachedIndices.put(psqlConnector.spaceId(), indexList);
      return indexList.indices;
    }

    IndexList(@Nullable List<@NotNull String> indices) {
      this.indices = indices;
      expiry = System.currentTimeMillis() + CACHE_INTERVAL_MS;
    }

    List<String> indices;
    long expiry;

    // Key: spaceId, value: indices!
    static Map<String, IndexList> cachedIndices = new HashMap<>();
  }

  public static IndexList rsHandler(ResultSet rs) {
    try {
      if (!rs.next()) {
        return new IndexList(null);
      }
      List<String> indices = new ArrayList<>();

      String result = rs.getString("idx_available");
      List<Map<String, Object>> raw =
          XyzSerializable.deserialize(result, new TypeReference<List<Map<String, Object>>>() {});
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
        else if (one.get("src").equals("o")) indices.add("o:" + (String) one.get("property"));
      }
      return new IndexList(indices);
    } catch (Exception e) {
      return new IndexList(null);
    }
  }
}
