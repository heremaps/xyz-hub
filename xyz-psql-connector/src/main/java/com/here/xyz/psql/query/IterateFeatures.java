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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.COMPOSITE_EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.tools.ECPSTool;
import com.here.xyz.util.db.SQLQuery;
import java.security.GeneralSecurityException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IterateFeatures extends SearchForFeatures<IterateFeaturesEvent, FeatureCollection> {
  private static final String HANDLE_ENCRYPTION_PHRASE = "IterateFeatures";
  protected long limit;
  protected long start;
  private String nextDataset = null;
  private String nextIOffset = "";
  private int numFeatures = 0;

  public IterateFeatures(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    super(event);
    limit = event.getLimit();
  }

  @Override
  protected SQLQuery buildQuery(IterateFeaturesEvent event) throws SQLException, ErrorResponseException {
    if (isCompositeQuery(event)) {
      SQLQuery extensionQuery = super.buildQuery(event);
      extensionQuery.setQueryFragment("filterWhereClause", "TRUE"); //TODO: Do not support search on iterate for now
      extensionQuery.setQueryFragment("iColumn", ", i, dataset");

      if (is2LevelExtendedSpace(event)) {
        int ds = 3;

        ds--;
        extensionQuery.setQueryFragment("iColumnBase", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetBase", buildIOffsetFragment(event, ds));

        ds--;
        extensionQuery.setQueryFragment("iColumnIntermediate", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetIntermediate", buildIOffsetFragment(event, ds));

        ds--;
        extensionQuery.setQueryFragment("iColumnExtension", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetExtension", buildIOffsetFragment(event, ds));
      }
      else {
        int ds = 2;

        ds--;
        extensionQuery.setQueryFragment("iColumnBase", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetBase", buildIOffsetFragment(event, ds));

        ds--;
        extensionQuery.setQueryFragment("iColumnExtension", buildIColumnFragment(ds));
        extensionQuery.setQueryFragment("iOffsetExtension", buildIOffsetFragment(event, ds));
      }
      extensionQuery.setNamedParameter("currentDataset", getDatasetFromHandle(event));

      SQLQuery query = new SQLQuery(
          "SELECT * FROM (${{extensionQuery}}) orderQuery ORDER BY dataset, i ${{limit}}");
      query.setQueryFragment("extensionQuery", extensionQuery);
      query.setQueryFragment("limit", buildLimitFragment(event.getLimit()));

      return query;
    }
    else {
      //It's not a composite space or only SUPER / EXTENSION is queried
      SQLQuery query = super.buildQuery(event);
      query.setQueryFragment("iColumn", ", i");

      boolean hasHandle = event.getHandle() != null;
      start = hasHandle ? Long.parseLong(event.getHandle()) : 0L;

      if (hasSearch) {
        if (hasHandle)
          query.setQueryFragment("offset", "OFFSET #{startOffset}");
      }
      else {
        if (hasHandle)
          query.setQueryFragment("filterWhereClause", "i > #{startOffset}");

        query.setQueryFragment("orderBy", "ORDER BY i");
      }

      if (hasHandle)
        query.setNamedParameter("startOffset", start);

      return query;
    }
  }

  protected boolean isCompositeQuery(IterateFeaturesEvent event) {
    return isExtendedSpace(event) && (event.getContext() == DEFAULT || event.getContext() == COMPOSITE_EXTENSION);
  }

  private static String buildIColumnFragment(int dataset) {
    return ", i, " + dataset + " as dataset";
  }

  private SQLQuery buildIOffsetFragment(IterateFeaturesEvent event, int dataset) {
    return new SQLQuery("AND " + dataset + " >= #{currentDataset} "
        + "AND (" + dataset + " > #{currentDataset} OR i > #{startOffset}) ORDER BY i")
        .withNamedParameter("startOffset", getIOffsetFromHandle(event));
  }

  private int getDatasetFromHandle(IterateFeaturesEvent event) {
    if (event.getHandle() == null)
      return -1;
    return Integer.parseInt(event.getHandle().split("_")[0]);
  }

  private int getIOffsetFromHandle(IterateFeaturesEvent event) {
    if (event.getHandle() == null)
      return 0;
    return Integer.parseInt(event.getHandle().split("_")[1]);
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    FeatureCollection fc = super.handle(rs);

    if (numFeatures > 0 && numFeatures == limit) {
      String nextHandle = (nextDataset != null ? nextDataset + "_" : "") + nextIOffset;
      fc.setHandle(nextHandle);
      fc.setNextPageToken(nextHandle);
    }

    if (!(this instanceof IterateFeaturesSorted) && hasSearch && fc.getHandle() != null) {
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
