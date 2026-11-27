/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static com.here.xyz.responses.XyzError.EXCEPTION;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;
import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.SearchForFeaturesEvent;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * Checks how database errors are mapped to the error types the Hub turns into HTTP status codes.
 */
class SQLExceptionMappingTest {

  private static ErrorResponseException map(SQLException e) {
    return assertThrows(ErrorResponseException.class,
        () -> new PSQLXyzConnector().handleProcessingException(e, new SearchForFeaturesEvent().withSpace("test-space")));
  }

  @Test
  void nulCharacterIsAnIllegalArgument() {
    ErrorResponseException e = map(new SQLException("ERROR: invalid byte sequence for encoding \"UTF8\": 0x00 Query: SELECT 1 "
        + "Parameters: [a\u0000b]", "22021"));
    assertEquals(ILLEGAL_ARGUMENT, e.getErrorResponse().getError());
    //The message must not echo the invalid input
    assertFalse(e.getErrorResponse().getErrorMessage().contains("\u0000"));
  }

  @Test
  void invalidH3CellIsAnIllegalArgument() {
    ErrorResponseException e = map(new SQLException("ERROR: Invalid h3Index 871faeba8fffff1: not a valid H3 cell Query: SELECT 1",
        "XYZ40"));
    assertEquals(ILLEGAL_ARGUMENT, e.getErrorResponse().getError());
    //The message must not echo the invalid input
    assertFalse(e.getErrorResponse().getErrorMessage().contains("871faeba8fffff1"));
  }

  @Test
  void missingH3ExtensionIsNotImplemented() {
    assertEquals(NOT_IMPLEMENTED, map(new SQLException("ERROR: function xyz_h3_cover(geometry, integer) does not exist Query: "
        + "SELECT 1", "42883")).getErrorResponse().getError());
    assertEquals(NOT_IMPLEMENTED, map(new SQLException("ERROR: schema \"h3_ext\" does not exist Query: SELECT 1", "3F000"))
        .getErrorResponse().getError());
    assertEquals(NOT_IMPLEMENTED, map(new SQLException("ERROR: type \"h3_ext.h3index\" does not exist", "42704"))
        .getErrorResponse().getError());
  }

  @Test
  void otherMissingFunctionsStayExceptions() {
    //Only the server's message counts, not the query text appended to it
    assertEquals(EXCEPTION, map(new SQLException("ERROR: function max_bigint() does not exist Query: SELECT "
        + "h3_ext.h3_is_pentagon(cc.h3)", "42883")).getErrorResponse().getError());
  }

  @Test
  void unknownStatesStayExceptions() {
    assertEquals(EXCEPTION, map(new SQLException("ERROR: something else", "XX001")).getErrorResponse().getError());
  }
}
