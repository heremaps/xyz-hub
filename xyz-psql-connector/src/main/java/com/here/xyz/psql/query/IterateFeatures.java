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

import static com.here.xyz.responses.XyzError.EXCEPTION;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.db.SQLQuery;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.crypto.IllegalBlockSizeException;

public class IterateFeatures<E extends IterateFeaturesEvent, R extends XyzResponse> extends SearchForFeatures<E, R> {
  protected long limit;
  private String nextDataset = null;
  private String nextIOffset = "";
  protected int numFeatures = 0;

  public IterateFeatures(E event) throws SQLException, ErrorResponseException {
    super(event);
    limit = event.getLimit();
  }

  @Override
  protected SQLQuery buildSelectClause(E event, int dataset) {
    return new SQLQuery("${{innerSelectClause}}, i")
        .withQueryFragment("innerSelectClause", super.buildSelectClause(event, dataset));
  }

  @Override
  protected SQLQuery buildFiltersFragment(E event, boolean isExtension, SQLQuery filterWhereClause, int dataset) {
    return new SQLQuery("${{innerFilters}} ${{offsetFilter}}")
        .withQueryFragment("innerFilters", super.buildFiltersFragment(event, isExtension, filterWhereClause, dataset))
        .withQueryFragment("offsetFilter", buildOffsetFilterFragment(event, dataset));
  }

  @Override
  protected SQLQuery buildFilterWhereClause(E event) {
    //NOTE: Search while iterating is not supported
    return new SQLQuery("TRUE");
  }

  @Override
  protected String buildOuterOrderByFragment(ContextAwareEvent event) {
    return "ORDER BY dataset, i";
  }

  @Override
  protected String buildOrderByFragment(ContextAwareEvent event) {
    return "ORDER BY i";
  }

  protected SQLQuery buildOffsetFilterFragment(IterateFeaturesEvent event, int dataset) {
    if (event.getNextPageToken() == null)
      return new SQLQuery("");

    TokenContent token = readTokenContent(event.getNextPageToken());
    return new SQLQuery("AND " + dataset + " >= #{currentDataset} "
        + "AND (" + dataset + " > #{currentDataset} OR i > #{startOffset})")
        .withNamedParameter("currentDataset", token.startDataset)
        .withNamedParameter("startOffset", token.startOffset);
  }

  private TokenContent readTokenContent(String token) {
    token = decodeToken(token);
    if (token.contains("_")) {
      final String[] tokenParts = token.split("_");
      return new TokenContent(Integer.parseInt(tokenParts[0]), Integer.parseInt(tokenParts[1]));
    }
    else
      return new TokenContent(-1, Long.parseLong(token));
  }

  private record TokenContent(int startDataset, long startOffset) {
    public String toString() {
      return startDataset + "_" + startOffset;
    }
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    FeatureCollection fc = (FeatureCollection) super.handle(rs);

    String nextToken = createNextPageToken();
    fc.setHandle(nextToken); //TODO: Kept for backwards compatibility - remove after deprecation period
    fc.setNextPageToken(nextToken);

    return (R) fc;
  }

  protected String createNextPageToken() {
    if (numFeatures > 0 && numFeatures == limit)
      return encodeToken((nextDataset != null ? nextDataset + "_" : "") + nextIOffset);
    return null;
  }

  @Override
  protected void handleFeature(ResultSet rs, StringBuilder result) throws SQLException {
    super.handleFeature(rs, result);
    numFeatures++;
    nextIOffset = rs.getString("i");
    nextDataset = rs.getString("dataset");
  }

  protected static String encodeToken(String tokenContent) {
    try {
      return ECPSTool.encrypt(IterateFeatures.class.getSimpleName(), tokenContent, true);
    }
    catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  protected static String decodeToken(String token) {
    try {
      return ECPSTool.decrypt(IterateFeatures.class.getSimpleName(), token, true);
    }
    catch (GeneralSecurityException e) {
      if (e.getCause() != null && e.getCause() instanceof IllegalBlockSizeException)
        throw new IllegalArgumentException("Invalid nextPageToken provided for iterate");
      throw new IllegalArgumentException("Error trying to decode the iteration nextPageToken.");
    }
  }
}
