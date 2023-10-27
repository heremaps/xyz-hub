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
package com.here.naksha.lib.psql.model;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.IAdvancedReadResult;
import com.here.naksha.lib.core.models.storage.ReadResult;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XyzFeatureReadResult<BASIC_FEATURE_TYPE extends XyzFeature> extends ReadResult<BASIC_FEATURE_TYPE>
    implements IAdvancedReadResult<BASIC_FEATURE_TYPE>, Iterator<BASIC_FEATURE_TYPE> {

  private static final Logger log = LoggerFactory.getLogger(XyzFeatureReadResult.class);

  private boolean hasNext = true;
  private final ResultSet rs;
  private String jsonData;
  private String propertiesType;
  private BASIC_FEATURE_TYPE feature;

  public XyzFeatureReadResult(@NotNull ResultSet resultSet, @NotNull Class<BASIC_FEATURE_TYPE> featureType) {
    super(featureType);
    this.rs = resultSet;
    movePointerToNext();
  }

  @Override
  public boolean hasNext() {
    try {
      return hasNext && !rs.isClosed();
    } catch (SQLException e) {
      log.atWarn()
          .setMessage("Unexpected exception while checking if next row is available")
          .setCause(e)
          .log();
      throw unchecked(e);
    }
  }

  @Override
  public boolean loadNext() {
    jsonData = null;
    feature = null;
    try {
      if (hasNext()) {
        try {
          final String jdata = rs.getString("jsondata");
          final String ptype = rs.getString("ptype");
          if (jdata != null) {
            this.jsonData = jdata;
            this.propertiesType = ptype;
          } else {
            hasNext = false;
            return false;
          }
          movePointerToNext();
          return true;
        } catch (Throwable t) {
          log.atWarn()
              .setMessage("Unexpected exception while processing next row from result-set")
              .setCause(t)
              .log();
        }
      }
    } catch (Throwable t) {
      log.atWarn()
          .setMessage("Unexpected exception while reading next row from result-set")
          .setCause(t)
          .log();
    }
    return false;
  }

  @Override
  public @NotNull BASIC_FEATURE_TYPE next() {
    if (loadNext()) {
      return getFeature();
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public @NotNull List<@NotNull BASIC_FEATURE_TYPE> next(int limit) {
    // FIXME maybe it can be optimized by reading 'n' elements at once by using rs.setFetchSize(n)
    List<BASIC_FEATURE_TYPE> result = new ArrayList<>(limit);
    int i = 0;
    while (hasNext() && i < limit) {
      result.add(next());
      i++;
    }
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public @NotNull <NT> IAdvancedReadResult<NT> withType(@NotNull Class<NT> featureClass) {
    super.withFeatureType(featureClass);
    return (IAdvancedReadResult<NT>) this;
  }

  @Override
  public @NotNull String getFeatureType() {
    return featureType.getTypeName();
  }

  @Override
  public @NotNull String getPropertiesType() {
    return propertiesType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public @NotNull BASIC_FEATURE_TYPE getFeature() {
    if (feature == null) {
      if (jsonData == null) {
        throw new NoSuchElementException();
      } else {
        feature = (BASIC_FEATURE_TYPE) featureOf(jsonData, featureType);
      }
    }
    return feature;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <NT> @NotNull NT getFeature(@NotNull Class<NT> featureClass) {
    if (jsonData == null) {
      throw new NoSuchElementException();
    }
    return featureOf(jsonData, featureClass);
  }

  @Override
  public IAdvancedReadResult<BASIC_FEATURE_TYPE> advanced() {
    return this;
  }

  @NotNull
  @Override
  public Iterator<BASIC_FEATURE_TYPE> iterator() {
    return this;
  }

  @Override
  protected void onFeatureTypeChange() {
    feature = null;
  }

  private void movePointerToNext() {
    try {
      if (hasNext) {
        this.hasNext = rs.next();
      }
    } catch (SQLException e) {
      log.atWarn()
          .setMessage("Unexpected exception while checking if next row is available")
          .setCause(e)
          .log();
      unchecked(e);
    }
  }
}
