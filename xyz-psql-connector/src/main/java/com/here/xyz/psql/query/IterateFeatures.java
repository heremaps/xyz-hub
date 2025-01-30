/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.db.SQLQuery;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IterateFeatures extends SearchForFeatures<IterateFeaturesEvent, FeatureCollection> {
  private static final String HANDLE_ENCRYPTION_PHRASE = "IterateFeatures";
  protected long limit;
  private boolean hasHandle;
  private long start;
  private int startDataset = -1;
  private String nextDataset = null;
  private String nextIOffset = "";
  private int numFeatures = 0;

  public IterateFeatures(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
    limit = event.getLimit();
    hasHandle = event.getHandle() != null;
    if (hasHandle)
      parseHandleContent(event.getHandle());
  }

  @Override
  protected SQLQuery buildSelectClause(IterateFeaturesEvent event, int dataset) {
    return new SQLQuery("${{innerSelectClause}}, i")
        .withQueryFragment("innerSelectClause", super.buildSelectClause(event, dataset));
  }

  @Override
  protected SQLQuery buildFiltersFragment(IterateFeaturesEvent event, boolean isExtension, SQLQuery filterWhereClause, int dataset) {
    final SQLQuery filtersFragment = super.buildFiltersFragment(event, isExtension, filterWhereClause, dataset);

    if (!isCompositeQuery(event))
      return filtersFragment;

    return new SQLQuery("${{innerFilters}} ${{offsetFilter}}")
        .withQueryFragment("innerFilters", filtersFragment)
        .withQueryFragment("offsetFilter", buildOffsetFilterFragment(event, dataset));
  }

  @Override
  protected SQLQuery buildFilterWhereClause(IterateFeaturesEvent event) {
    if (isCompositeQuery(event))
      return new SQLQuery("TRUE"); //TODO: Do not support search on iterate for now

    if (!hasSearch && event.getHandle() != null)
      return new SQLQuery("i > #{startOffset}")
          .withNamedParameter("startOffset", start);

    return super.buildFilterWhereClause(event);
  }

  @Override
  protected String buildOuterOrderByFragment(ContextAwareEvent event) {
    if (hasSearch && hasHandle)
      return super.buildOrderByFragment(event);

    return "ORDER BY dataset, i";
  }

  @Override
  protected String buildOrderByFragment(ContextAwareEvent event) {
    if (hasSearch && hasHandle)
      return super.buildOrderByFragment(event);

    return "ORDER BY i";
  }

  private SQLQuery buildOffsetFilterFragment(IterateFeaturesEvent event, int dataset) {
    return new SQLQuery("AND " + dataset + " >= #{currentDataset} "
        + "AND (" + dataset + " > #{currentDataset} OR i > #{startOffset})")
        .withNamedParameter("currentDataset", startDataset)
        .withNamedParameter("startOffset", start);
  }

  @Override
  protected SQLQuery buildLimitFragment(IterateFeaturesEvent event) {
    if (hasSearch && hasHandle)
      return new SQLQuery("${{innerLimit}} OFFSET #{startOffset}")
          .withQueryFragment("innerLimit", super.buildLimitFragment(event))
          .withNamedParameter("startOffset", start);

    return super.buildLimitFragment(event);
  }

  private void parseHandleContent(String handle) {
    if (handle.contains("_")) {
      startDataset = getDatasetFromHandle(handle);
      start = getIOffsetFromHandle(handle);
    }
    else
      start = Long.parseLong(handle);
  }

  private int getDatasetFromHandle(String handle) {
    return Integer.parseInt(handle.split("_")[0]);
  }

  private int getIOffsetFromHandle(String handle) {
    return Integer.parseInt(handle.split("_")[1]);
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    FeatureCollection fc = super.handle(rs);

    if (numFeatures > 0 && numFeatures == limit) {
      String nextHandle = (nextDataset != null ? nextDataset + "_" : "") + nextIOffset;
      fc.setHandle(nextHandle);
      fc.setNextPageToken(nextHandle);
    }

    if (hasSearch && fc.getHandle() != null) {
      fc.setHandle("" + (start + limit)); //Kept for backwards compatibility for now
      fc.setNextPageToken("" + (start + limit));
    }

    return fc;
  }

  @Override
  protected void handleFeature(ResultSet rs, StringBuilder result) throws SQLException {
    super.handleFeature(rs, result);
    numFeatures++;
    nextIOffset = rs.getString("i");
    if (rs.getMetaData().getColumnCount() >= 5)
      nextDataset = rs.getString("dataset");
  }

  protected static String encryptHandle(String plainText) throws GeneralSecurityException {
    return ECPSTool.encrypt(HANDLE_ENCRYPTION_PHRASE, plainText, true);
  }

  protected static String decryptHandle(String encryptedText) throws GeneralSecurityException {
    return ECPSTool.decrypt(HANDLE_ENCRYPTION_PHRASE, encryptedText, true);
  }
}
