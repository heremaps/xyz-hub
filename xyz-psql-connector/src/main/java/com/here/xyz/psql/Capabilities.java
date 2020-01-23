/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

public class Capabilities {

  /**
   * Determines if PropertiesQuery can be executed. Check if required Indices are created.
   */
  public static boolean canSearchFor(String space, PropertiesQuery query, PSQLXyzConnector connector) {
    if (query == null) {
      return true;
    }

    try {
      List<String> keys = query.stream().flatMap(List::stream)
          .filter(k -> k.getKey() != null && k.getKey().length() > 0).map(PropertyQuery::getKey).collect(Collectors.toList());

      int idx_check = 0;

      for (String key : keys) {
        if (key.equals("id")) {
          return true;
        }
        String[] parts = key.split("\\.");
        if (parts.length < 2 || !parts[0].equals("properties")) {
          continue;
        }

        if (parts.length == 3 && parts[1].equals("@ns:com:here:xyz") && (parts[2].equals("createdAt") || parts[2].equals("updatedAt"))) {
          return true;
        }
        if (parts.length >= 2) {
          List<String> indices = IndexList.getIndexList(space, connector);

          // The table is small and not indexed. It's not listed in the xyz_idxs_status table
          if (indices == null) {
            return true;
          }

          if (indices.contains(key.substring("properties.".length()))) {
        	  /** Check if all properties are indexed */
        	  idx_check++;
          }
        }
      }

      if(idx_check == keys.size())
    	  return true;

      return IndexList.getIndexList(space, connector) == null;
    } catch (Exception e) {
      // In all cases, when something with the check went wrong, allow the search
      return true;
    }
  }

  public static class IndexList {
    /** Cache indexList for 3 Minutes  */
    static long CACHE_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

    /** Get list of indexed Values from a XYZ-Space */
    static List<String> getIndexList(String space, PSQLXyzConnector connector) throws SQLException {
      IndexList indexList = cachedIndices.get(space);
      if (indexList != null && indexList.expiry >= System.currentTimeMillis()) {
        return indexList.indices;
      }

      indexList = connector.executeQuery(new SQLQuery("SELECT idx_available FROM xyz_config.xyz_idxs_status WHERE spaceid=?", space),
              Capabilities::rsHandler);

      cachedIndices.put(space, indexList);
      return indexList.indices;
    }

    IndexList(List<String> indices) {
      this.indices = indices;
      expiry = System.currentTimeMillis() + CACHE_INTERVAL_MS;
    }

    List<String> indices;
    long expiry;

    static Map<String, IndexList> cachedIndices = new HashMap<>();
  }

  public static IndexList rsHandler(ResultSet rs) {
    try {
      if (!rs.next()) {
        return new IndexList(null);
      }
      List<String> indices = new ArrayList<>();

      String result = rs.getString("idx_available");
      List<Map<String, Object>> raw = XyzSerializable.deserialize(result, new TypeReference<List<Map<String, Object>>>() {});
      for (Map<String, Object> one : raw) {
        /**
         * Indices are marked as:
         * a = automatically created (auto-indexing)
         * m = manually created (on-demand)
         * s = basic system indices
         */
        if (one.get("src").equals("a") || one.get("src").equals("m")) {
          indices.add((String) one.get("property"));
        }
      }

      return new IndexList(indices);
    } catch (Exception e) {
      return new IndexList(null);
    }
  }
}
