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
package com.here.naksha.lib.psql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.intThat;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class PsqlCursorTest {

  @Test
  void testCursorCodecChange() throws SQLException {
    // given
    Statement statement = Mockito.mock(Statement.class);
    ResultSet rs = Mockito.mock(ResultSet.class);
    StringCodecFactory stringCodecFactory = new StringCodecFactory();
    XyzFeatureCodecFactory xyzFeatureCodecFactory = XyzFeatureCodecFactory.get();

    PsqlCursor<XyzFeature, XyzFeatureCodec> cursor = new PsqlCursor<>(xyzFeatureCodecFactory, null, statement, rs);

    // when
    when(rs.next()).thenReturn(true);

    when(rs.getString(intThat(i -> i < 6))).thenReturn("id");
    when(rs.getString(6)).thenReturn("feature");
    when(rs.getString(8)).thenReturn(null);
    ForwardCursor<String, StringCodec> forwardCursor = cursor.withCodecFactory(stringCodecFactory, true);

    // expect
    assertEquals(stringCodecFactory, cursor.getCodecFactory());
    assertTrue(forwardCursor.next());
    assertEquals("feature", forwardCursor.getFeature());
  }

  @ParameterizedTest
  @MethodSource("psqlErrorValues")
  void testPsqlError(String psqlError, XyzError expected) throws SQLException {
    // given
    String responseJson = "{\"err\": \"" + psqlError + "\", \"msg\": \"Error Message\"}";

    // when
    PsqlCursor<XyzFeature, XyzFeatureCodec> cursor = createCursorWithMockedError(responseJson);

    // expect
    assertTrue(cursor.next());
    assertTrue(cursor.hasError());
    assertNotNull(cursor.getError());
    assertEquals(expected, cursor.getError().err);
    assertEquals("Error Message", cursor.getError().msg);
    assertEquals("{}", cursor.getJson());
  }

  private static Stream<Arguments> psqlErrorValues() {
    return Stream.of(
        Arguments.of("N0000", XyzError.EXCEPTION),
        Arguments.of("N0001", XyzError.CONFLICT),
        Arguments.of("N0002", XyzError.COLLECTION_NOT_FOUND),
        Arguments.of("23514", XyzError.EXCEPTION),
        Arguments.of("22023", XyzError.ILLEGAL_ARGUMENT),
        Arguments.of("23505", XyzError.CONFLICT),
        Arguments.of("02000", XyzError.NOT_FOUND),
        Arguments.of("UNKNOWN_CODE", XyzError.get("UNKNOWN_CODE")));
  }

  private PsqlCursor<XyzFeature, XyzFeatureCodec> createCursorWithMockedError(String errJson) throws SQLException {
    Statement statement = Mockito.mock(Statement.class);
    ResultSet rs = Mockito.mock(ResultSet.class);
    XyzFeatureCodecFactory xyzFeatureCodecFactory = XyzFeatureCodecFactory.get();

    PsqlCursor<XyzFeature, XyzFeatureCodec> cursor = new PsqlCursor<>(xyzFeatureCodecFactory, null, statement, rs);

    when(rs.next()).thenReturn(true);

    when(rs.getString(intThat(i -> i < 6))).thenReturn("id");
    when(rs.getString(6)).thenReturn("{}");
    when(rs.getString(8)).thenReturn(errJson);

    return cursor;
  }
}
