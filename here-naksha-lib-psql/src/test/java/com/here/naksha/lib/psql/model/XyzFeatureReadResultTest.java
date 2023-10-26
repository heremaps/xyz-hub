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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.models.features.Catalog;
import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.IAdvancedReadResult;
import com.here.naksha.lib.core.models.storage.ReadResult;
import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class XyzFeatureReadResultTest {
  private static final DbResult[] EMPTY = new DbResult[0];
  private static final String SAMPLE_CATALOG_JSON = "{\"id\": 1, \"type\": \"Catalog\"}";

  @Test
  void testIterator() throws SQLException {
    // given
    DbResult dbResult1 = dbResult("{\"id\": 3, \"type\": \"Catalog\"}");
    DbResult dbResult2 = dbResult("{\"id\": 2, \"type\": \"Catalog\"}");

    ResultSet rsMock = createResultSet(dbResult1, dbResult2);

    ReadResult<Catalog> results = new XyzFeatureReadResult<>(rsMock, Catalog.class);

    // when
    List<Catalog> features = new ArrayList<>();
    for (Catalog feature : results) {
      features.add(feature);
    }

    // then
    Assertions.assertEquals(2, features.size());
    Assertions.assertEquals("3", features.get(0).getId());
    Assertions.assertEquals("2", features.get(1).getId());
  }

  @Test
  void testHasNext() throws SQLException {
    // given
    ResultSet rsMock = createResultSet(EMPTY);
    when(rsMock.isAfterLast()).thenReturn(true);

    // when
    IAdvancedReadResult<Catalog> result = new XyzFeatureReadResult<>(rsMock, Catalog.class);

    // then
    Assertions.assertFalse(result.hasNext());
    Assertions.assertFalse(result.loadNext());
  }

  @Test()
  void shouldThrowExceptionWhenReadingNotLoadedFeature() throws SQLException {
    // given
    ResultSet rsMock = createResultSet(EMPTY);

    // when
    IAdvancedReadResult<Catalog> result = new XyzFeatureReadResult<>(rsMock, Catalog.class);

    // then
    Assertions.assertThrows(NoSuchElementException.class, result::getFeature);
    Assertions.assertThrows(NoSuchElementException.class, () -> result.getFeature(Catalog.class));
  }

  @Test
  void shouldReturnLimitedFeatures() throws SQLException {
    // given
    DbResult[] twentySizeRs = new DbResult[20];
    Arrays.fill(twentySizeRs, dbResult(SAMPLE_CATALOG_JSON));
    ResultSet rsMock = createResultSet(twentySizeRs);

    // when
    IAdvancedReadResult<Catalog> result = new XyzFeatureReadResult<>(rsMock, Catalog.class);
    List<Catalog> features = result.next(10);

    // then
    Assertions.assertEquals(10, features.size());
  }

  @Test
  void shouldReturnAllFeaturesWhenLimitIsGreaterThanNumberOfElements() throws SQLException {
    // given
    DbResult[] twentySizeRs = new DbResult[20];
    Arrays.fill(twentySizeRs, dbResult(SAMPLE_CATALOG_JSON));
    ResultSet rsMock = createResultSet(twentySizeRs);

    // when
    IAdvancedReadResult<Catalog> result = new XyzFeatureReadResult<>(rsMock, Catalog.class);
    List<Catalog> features = result.next(100);

    // then
    Assertions.assertEquals(20, features.size());
  }

  @Test
  void shouldReturnEmptyListInLimitedNextWhenResultSetIsEmpty() throws SQLException {
    // given
    ResultSet rsMock = createResultSet(EMPTY);

    // when
    IAdvancedReadResult<Catalog> result = new XyzFeatureReadResult<>(rsMock, Catalog.class);
    List<Catalog> features = result.next(10);

    // then
    Assertions.assertEquals(Collections.EMPTY_LIST, features);
  }

  @Test
  void shouldBeAbleToReadFeatureUsingDifferentTypeThanSpecifiedAtBegining() throws SQLException {
    // given
    ResultSet rsMock = createResultSet(dbResult(SAMPLE_CATALOG_JSON));

    // when
    IAdvancedReadResult<NakshaFeature> result = new XyzFeatureReadResult<>(rsMock, NakshaFeature.class);
    result.loadNext();
    Catalog catalogTypeResult = result.getFeature(Catalog.class);

    // then
    Assertions.assertEquals("1", catalogTypeResult.getId());
  }

  @Test
  void shouldBeAbleToChangeResultSetType() throws SQLException {
    // given
    ResultSet rsMock = createResultSet(dbResult(SAMPLE_CATALOG_JSON));

    // when
    ReadResult<NakshaFeature> result = new XyzFeatureReadResult<>(rsMock, NakshaFeature.class);
    IAdvancedReadResult<Catalog> changedTypeResult =
        result.withFeatureType(Catalog.class).advanced();
    changedTypeResult.loadNext();
    Catalog featureTypeChanged = changedTypeResult.getFeature();

    // then
    Assertions.assertNotNull(featureTypeChanged);
  }

  @Test
  void shouldBeAbleToReturnDifferentTypes() throws SQLException {
    // given
    DbResult dbResult1 = dbResult("{\"id\": 3, \"type\": \"Catalog\"}");
    DbResult dbResult2 = dbResult("{\"id\": 2, \"type\": \"Storage\"}");

    ResultSet rsMock = createResultSet(dbResult1, dbResult2);

    ReadResult<Catalog> results = new XyzFeatureReadResult<>(rsMock, Catalog.class);

    // when
    results.advanced().loadNext();
    Catalog catalog = results.advanced().getFeature();
    results.advanced().loadNext();
    Storage feature = results.advanced().getFeature(Storage.class);

    // then
    Assertions.assertEquals("3", catalog.getId());
    Assertions.assertEquals("2", feature.getId());
  }

  private ResultSet createResultSet(DbResult... results) throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    Boolean isEmpty = results.length == 0;

    Boolean[] falseArrEndedWithTrue = new Boolean[results.length];
    Arrays.fill(falseArrEndedWithTrue, false);
    if (!isEmpty) {
      falseArrEndedWithTrue[falseArrEndedWithTrue.length - 1] = true;
    }

    when(rs.next()).thenReturn(!isEmpty);
    when(rs.isAfterLast()).thenReturn(isEmpty, falseArrEndedWithTrue);
    when(rs.isClosed()).thenReturn(isEmpty);

    if (!isEmpty) {
      when(rs.getString("jsondata"))
          .thenReturn(results[0].jsonData, getTailOf(results, DbResult::getJsonData, String.class));
      when(rs.getString("ptype"))
          .thenReturn(results[0].pType, getTailOf(results, DbResult::getJsonData, String.class));
    }

    return rs;
  }

  private <R> R[] getTailOf(DbResult[] results, Function<DbResult, R> mapper, Class<R> clazz) {
    if (results.length > 1) {
      return Arrays.stream(results).skip(1).map(mapper).toList().toArray((R[]) Array.newInstance(clazz, 0));
    }
    return (R[]) Array.newInstance(clazz, 0);
  }

  class DbResult {
    String jsonData;
    String pType = "";

    public String getJsonData() {
      return jsonData;
    }

    public String getpType() {
      return pType;
    }
  }

  private DbResult dbResult(String json) throws SQLException {
    DbResult dbResult = new DbResult();
    dbResult.jsonData = json;
    return dbResult;
  }
}
