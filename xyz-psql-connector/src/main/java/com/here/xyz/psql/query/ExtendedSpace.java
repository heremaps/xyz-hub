/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import com.here.xyz.events.Event;
import com.here.xyz.psql.PsqlEventHandler;
import com.here.xyz.responses.XyzResponse;
import java.sql.SQLException;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class ExtendedSpace<E extends Event, R extends XyzResponse> extends XyzQueryRunner<E, R> {

  private static final String EXTENDS = "extends";
  private static final String SPACE_ID = "spaceId";

  public ExtendedSpace(E event, final @NotNull PsqlEventHandler psqlConnector) throws SQLException, ErrorResponseException {
    super(event, psqlConnector);
  }

  @SuppressWarnings("unchecked")
  protected static @NotNull Map<@NotNull String, @Nullable Object> getExtends(@NotNull Event event) {
    final Object raw = event.getParams().get(EXTENDS);
    assert raw instanceof Map;
    return ((Map<String, Object>) raw);
  }

  protected static <E extends Event> boolean isExtendedSpace(@NotNull E event) {
    return false;
  }

  protected static <E extends Event> boolean is2LevelExtendedSpace(@NotNull E event) {
    return false;
  }

  private static <E extends Event> String getFirstLevelExtendedTable(@NotNull E event, @NotNull PsqlEventHandler processor) {
    return null;
  }

  private static <E extends Event> String getSecondLevelExtendedTable(@NotNull E event, @NotNull PsqlEventHandler processor) {
    return null;
  }

  public static <E extends Event> String getExtendedTable(@NotNull E event, @NotNull PsqlEventHandler processor) {
    return null;
  }

  protected String getExtendedTable(E event) {
    return getExtendedTable(event, processor);
  }

  protected static <E extends Event> String getIntermediateTable(@NotNull E event, @NotNull PsqlEventHandler psqlConnector) {
    return null;
  }

  protected String getIntermediateTable(E event) {
    return getIntermediateTable(event, processor);
  }
}
