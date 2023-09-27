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

import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.tools.DhString;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;

public class FeatureResultSetHandler implements ResultSetHandler<FeatureCollection> {

  /**
   * The default handler for the most results.
   *
   * @param rs the result set.
   * @return the generated feature collection from the result set.
   * @throws SQLException when any unexpected error happened.
   */

  public static final long MAX_RESULT_CHARS = 100 * 1024 *1024;
  private boolean skipNullGeom;
  private boolean useColumnNames;
  private long iterationLimit;

  public FeatureResultSetHandler(boolean skipNullGeom, boolean useColumnNames) {
    this(skipNullGeom, useColumnNames, -1);
  }

  public FeatureResultSetHandler(boolean skipNullGeom, boolean useColumnNames, long iterationLimit) {
    this.skipNullGeom = skipNullGeom;
    this.useColumnNames = useColumnNames;
    this.iterationLimit = iterationLimit;
  }

  private static String getGeoFromResultSet(ResultSet rs, boolean useColumnName) throws SQLException {
    return useColumnName ? rs.getString("geo") : rs.getString(2);
  }

  private static String getJsondataFromResultSet(ResultSet rs, boolean useColumnName) throws SQLException {
    return useColumnName ? rs.getString("jsondata") : rs.getString(1);
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    //TODO: Use column names everywhere rather than column indices
    //TODO: Move iteration specific parts into a special handler inside IterateFeatures QR
    String nextIOffset = "";
    String nextDataset = null;

    StringBuilder sb = new StringBuilder();
    String prefix = "[";
    sb.append(prefix);
    int numFeatures = 0;

    while (rs.next() && MAX_RESULT_CHARS > sb.length()) {
      String geom = getGeoFromResultSet(rs, useColumnNames);
      if (skipNullGeom && geom == null)
        continue;
      sb.append(getJsondataFromResultSet(rs, useColumnNames));
      sb.setLength(sb.length() - 1);
      sb.append(",\"geometry\":");
      sb.append(geom == null ? "null" : geom);
      sb.append("}");
      sb.append(",");

      if (iterationLimit != -1) {
        numFeatures++;
        nextIOffset = rs.getString(3);
        if (rs.getMetaData().getColumnCount() >= 5)
          nextDataset = rs.getString("dataset");
      }
    }

    if (sb.length() > prefix.length()) {
      sb.setLength(sb.length() - 1);
    }
    sb.append("]");

    final FeatureCollection featureCollection = new FeatureCollection();
    featureCollection._setFeatures(sb.toString());

    if (sb.length() > MAX_RESULT_CHARS) throw new SQLException(DhString.format("Maxchar limit(%d) reached", MAX_RESULT_CHARS));

    if (iterationLimit != -1 && numFeatures > 0 && numFeatures == iterationLimit) {
      String nextHandle = (nextDataset != null ? nextDataset + "_" : "") + nextIOffset;
      featureCollection.setHandle(nextHandle);
      featureCollection.setNextPageToken(nextHandle);
    }

    return featureCollection;
  }
}
