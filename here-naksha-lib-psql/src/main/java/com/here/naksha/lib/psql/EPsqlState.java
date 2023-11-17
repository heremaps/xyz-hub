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

import com.here.naksha.lib.core.util.json.JsonEnum;

/**
 * The PSQL error states.
 */
@SuppressWarnings("unused")
public class EPsqlState extends JsonEnum {
  // https://www.postgresql.org/docs/current/errcodes-appendix.html


  public static final EPsqlState OK = def(EPsqlState.class, "00000");
  public static final EPsqlState TOO_MANY_RESULTS = def(EPsqlState.class, "0100E");
  public static final EPsqlState NO_DATA = def(EPsqlState.class, "02000");
  public static final EPsqlState INVALID_PARAMETER_TYPE = def(EPsqlState.class, "07006");

  /**
   * We could establish a connection with the server for unknown reasons. Could be a network problem.
   */
  public static final EPsqlState CONNECTION_UNABLE_TO_CONNECT = def(EPsqlState.class, "08001");

  public static final EPsqlState CONNECTION_DOES_NOT_EXIST = def(EPsqlState.class, "08003");

  /**
   * The server rejected our connection attempt. Usually an authentication failure, but could be a configuration error like asking for a SSL
   * connection with a server that wasn't built with SSL support.
   */
  public static final EPsqlState CONNECTION_REJECTED = def(EPsqlState.class, "08004");

  /**
   * After a connection has been established, it went bad.
   */
  public static final EPsqlState CONNECTION_FAILURE = def(EPsqlState.class, "08006");

  public static final EPsqlState CONNECTION_FAILURE_DURING_TRANSACTION = def(EPsqlState.class, "08007");

  /**
   * The server sent us a response the driver was not prepared for and is either bizarre datastream corruption, a driver bug, or a protocol
   * violation on the server's part.
   */
  public static final EPsqlState PROTOCOL_VIOLATION = def(EPsqlState.class, "08P01");

  public static final EPsqlState COMMUNICATION_ERROR = def(EPsqlState.class, "08S01");
  public static final EPsqlState NOT_IMPLEMENTED = def(EPsqlState.class, "0A000");
  public static final EPsqlState DATA_ERROR = def(EPsqlState.class, "22000");
  public static final EPsqlState STRING_DATA_RIGHT_TRUNCATION = def(EPsqlState.class, "22001");
  public static final EPsqlState NUMERIC_VALUE_OUT_OF_RANGE = def(EPsqlState.class, "22003");
  public static final EPsqlState BAD_DATETIME_FORMAT = def(EPsqlState.class, "22007");
  public static final EPsqlState DATETIME_OVERFLOW = def(EPsqlState.class, "22008");
  public static final EPsqlState DIVISION_BY_ZERO = def(EPsqlState.class, "22012");
  public static final EPsqlState MOST_SPECIFIC_TYPE_DOES_NOT_MATCH = def(EPsqlState.class, "2200G");
  public static final EPsqlState INVALID_PARAMETER_VALUE = def(EPsqlState.class, "22023");
  public static final EPsqlState NOT_NULL_VIOLATION = def(EPsqlState.class, "23502");
  public static final EPsqlState FOREIGN_KEY_VIOLATION = def(EPsqlState.class, "23503");
  public static final EPsqlState UNIQUE_VIOLATION = def(EPsqlState.class, "23505");
  public static final EPsqlState CHECK_VIOLATION = def(EPsqlState.class, "23514");
  public static final EPsqlState EXCLUSION_VIOLATION = def(EPsqlState.class, "23P01");
  public static final EPsqlState INVALID_CURSOR_STATE = def(EPsqlState.class, "24000");
  public static final EPsqlState TRANSACTION_STATE_INVALID = def(EPsqlState.class, "25000");
  public static final EPsqlState ACTIVE_SQL_TRANSACTION = def(EPsqlState.class, "25001");
  public static final EPsqlState NO_ACTIVE_SQL_TRANSACTION = def(EPsqlState.class, "25P01");
  public static final EPsqlState IN_FAILED_SQL_TRANSACTION = def(EPsqlState.class, "25P02");
  public static final EPsqlState INVALID_SQL_STATEMENT_NAME = def(EPsqlState.class, "26000");
  public static final EPsqlState INVALID_AUTHORIZATION_SPECIFICATION = def(EPsqlState.class, "28000");
  public static final EPsqlState INVALID_PASSWORD = def(EPsqlState.class, "28P01");
  public static final EPsqlState INVALID_TRANSACTION_TERMINATION = def(EPsqlState.class, "2D000");
  public static final EPsqlState STATEMENT_NOT_ALLOWED_IN_FUNCTION_CALL = def(EPsqlState.class, "2F003");
  public static final EPsqlState INVALID_SAVEPOINT_SPECIFICATION = def(EPsqlState.class, "3B000");
  public static final EPsqlState INVALID_SCHEMA_NAME = def(EPsqlState.class, "3F000");
  public static final EPsqlState SERIALIZATION_FAILURE = def(EPsqlState.class, "40001");
  public static final EPsqlState DEADLOCK_DETECTED = def(EPsqlState.class, "40P01");
  public static final EPsqlState SYNTAX_ERROR = def(EPsqlState.class, "42601");
  public static final EPsqlState UNDEFINED_COLUMN = def(EPsqlState.class, "42703");
  public static final EPsqlState UNDEFINED_OBJECT = def(EPsqlState.class, "42704");
  public static final EPsqlState WRONG_OBJECT_TYPE = def(EPsqlState.class, "42809");
  public static final EPsqlState NUMERIC_CONSTANT_OUT_OF_RANGE = def(EPsqlState.class, "42820");
  public static final EPsqlState DATA_TYPE_MISMATCH = def(EPsqlState.class, "42821");
  public static final EPsqlState UNDEFINED_FUNCTION = def(EPsqlState.class, "42883");
  public static final EPsqlState INVALID_NAME = def(EPsqlState.class, "42602");
  public static final EPsqlState DATATYPE_MISMATCH = def(EPsqlState.class, "42804");
  public static final EPsqlState CANNOT_COERCE = def(EPsqlState.class, "42846");
  public static final EPsqlState UNDEFINED_TABLE = def(EPsqlState.class, "42P01");
  public static final EPsqlState INVALID_SCHEMA_DEFINITION = def(EPsqlState.class, "42P15");
  public static final EPsqlState OUT_OF_MEMORY = def(EPsqlState.class, "53200");
  public static final EPsqlState OBJECT_NOT_IN_STATE = def(EPsqlState.class, "55000");
  public static final EPsqlState OBJECT_IN_USE = def(EPsqlState.class, "55006");
  public static final EPsqlState QUERY_CANCELED = def(EPsqlState.class, "57014");
  public static final EPsqlState SYSTEM_ERROR = def(EPsqlState.class, "60000");
  public static final EPsqlState IO_ERROR = def(EPsqlState.class, "58030");
  public static final EPsqlState UNEXPECTED_ERROR = def(EPsqlState.class, "99999");
  public static final EPsqlState DUPLICATE_TABLE = def(EPsqlState.class, "42P07");
  public static final EPsqlState DUPLICATE_OBJECT = def(EPsqlState.class, "42710");

  public boolean isConnectionError() {
    return this == CONNECTION_UNABLE_TO_CONNECT
        || this == CONNECTION_DOES_NOT_EXIST
        || this == CONNECTION_REJECTED
        || this == CONNECTION_FAILURE
        || this == CONNECTION_FAILURE_DURING_TRANSACTION;
  }

  @Override
  protected void init() {
    register(EPsqlState.class);
  }
}
