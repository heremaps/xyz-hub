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

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PsqlReadSessionTest {

  static {
    NakshaContext.currentContext().setAppId("app_id");
    NakshaContext.currentContext().setAuthor("author");
  }

  // Dummy connection, DB doesn't have to be up and running
  final PsqlConfig config = new PsqlConfigBuilder()
      .withAppName("Naksha-Psql-Test")
      .parseUrl("jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=dummy")
      .build();
  PsqlStorage storage = new PsqlStorage(config, 0L);

  @Test
  void testStatementTimeoutConversion() {
    try (PsqlReadSession readSession = new PsqlReadSession(storage, defaultConnectionMock())) {
      // given
      readSession.setStatementTimeout(15000, TimeUnit.MILLISECONDS);

      // when
      Long result = readSession.getStatementTimeout(TimeUnit.SECONDS);

      // then
      Assertions.assertEquals(result, 15);
    }
  }

  @Test
  void testLockTimeoutConversion() {
    try (PsqlReadSession readSession = new PsqlReadSession(storage, defaultConnectionMock())) {
      // given
      readSession.setStatementTimeout(3, TimeUnit.SECONDS);

      // when
      Long result = readSession.getStatementTimeout(TimeUnit.MILLISECONDS);

      // then
      Assertions.assertEquals(result, 3000);
    }
  }

  @Test
  void shouldThrowExceptionForUnsupportedReadRequests() {
    try (PsqlReadSession readSession = new PsqlReadSession(storage, defaultConnectionMock())) {
      // expect
      Assertions.assertThrows(
          UnsupportedOperationException.class, () -> readSession.execute(new DummyReadRequest()));
    }
  }

  private Connection defaultConnectionMock() {
    Connection mockConn = mock(Connection.class);
    try {
      Statement mockStmt = mock(Statement.class);
      PreparedStatement mockPreparedStmt = mock(PreparedStatement.class);
      when(mockConn.createStatement()).thenReturn(mockStmt);
      when(mockConn.prepareStatement(anyString())).thenReturn(mockPreparedStmt);
    } catch (Throwable t) {
      unchecked(t);
    }
    return mockConn;
  }

  static class DummyReadRequest extends ReadRequest<DummyReadRequest> {}
}
