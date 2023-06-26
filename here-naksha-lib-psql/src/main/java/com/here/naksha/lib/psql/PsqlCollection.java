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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.naksha.lib.core.view.Member.Export;
import com.here.naksha.lib.core.view.Member.Import.User;
import org.jetbrains.annotations.NotNull;

/**
 * A PostgresQL collection is a physical storage as understood by Naksha PostgresQL library. The
 * same collection can have multiple space identifiers, but it only has one unique consistent
 * collection identifier.
 */
public class PsqlCollection {
  // Note: Meta information attached JSON serialized to collection tables in PostgresQL.
  //       COMMENT ON TABLE test IS 'Some table';
  //       SELECT pg_catalog.obj_description('test'::regclass, 'pg_class');
  //       Comments can also be added on other objects, like columns, data types, functions, etc.
  // See:
  // https://stackoverflow.com/questions/17947274/is-it-possible-to-add-table-metadata-in-postgresql

  public static final String TYPE = "type";
  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String MAX_AGE = "maxAge";
  public static final String HISTORY = "history";
  public static final String DELETED_AT = "deleted";

  /**
   * Creates a new PSQL collection.
   *
   * @param schema The schema.
   * @param table The table name, being as well the unique identifier of the collection.
   */
  @JsonCreator
  public PsqlCollection(@JsonProperty @NotNull String schema, @JsonProperty @NotNull String table) {
    this.schema = schema;
    this.table = table;
  }

  /** The type identifier. */
  @JsonProperty(TYPE)
  @JsonInclude(Include.ALWAYS)
  public final @NotNull String type = "Collection";

  /** The schema of the collection. */
  @JsonProperty(SCHEMA)
  @JsonView({Export.User.class, User.class}) // We do not import/export to storage, so to comment of table!
  public final @NotNull String schema;

  /** The table name, being the collection identifier. */
  @JsonProperty(TABLE)
  @JsonView({Export.User.class, User.class}) // We do not import/export to storage, so to comment of table!
  public final @NotNull String table;

  /**
   * The maximum age of the history entries in days. Zero means no history, {@link Long#MAX_VALUE}
   * means unlimited.
   */
  @JsonProperty(MAX_AGE)
  @JsonInclude(Include.ALWAYS)
  public long maxAge = Long.MAX_VALUE;

  /** Toggle if the history is enabled. */
  @JsonProperty(HISTORY)
  @JsonInclude(Include.ALWAYS)
  public boolean history = Boolean.TRUE;

  /**
   * A value greater than zero implies that the collection shall be treated as deleted and
   * represents the UTC Epoch timestamp in milliseconds when the deletion has been done.
   */
  @JsonProperty(DELETED_AT)
  @JsonInclude(Include.NON_EMPTY)
  public long deletedAt = 0L;
}
