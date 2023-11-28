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

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
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

  @Test
  void testErrorRead() throws SQLException {
    // given
    Statement statement = Mockito.mock(Statement.class);
    ResultSet rs = Mockito.mock(ResultSet.class);
    XyzFeatureCodecFactory xyzFeatureCodecFactory = XyzFeatureCodecFactory.get();

    PsqlCursor<XyzFeature, XyzFeatureCodec> cursor = new PsqlCursor<>(xyzFeatureCodecFactory, null, statement, rs);

    // when
    when(rs.next()).thenReturn(true);

    when(rs.getString(intThat(i -> i < 6))).thenReturn("id");
    when(rs.getString(6)).thenReturn("{}");
    when(rs.getString(8)).thenReturn("{\"err\": \"23505\", \"msg\": \"Error Message\"}");

    // then
    assertTrue(cursor.next());
    assertTrue(cursor.hasError());
    assertNotNull(cursor.getError());
    assertEquals("23505", cursor.getError().err.value());
    assertEquals("Error Message", cursor.getError().msg);
    assertEquals("{}", cursor.getJson());
  }
}
