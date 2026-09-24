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

package com.here.xyz.psql.query;

import static com.here.xyz.models.hub.Space.TABLE_NAME;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.responses.XyzResponse;
import java.sql.SQLException;
import java.util.Map;

public abstract class ExtendedSpace<E extends Event, R extends XyzResponse> extends XyzQueryRunner<E, R> {

  private static final String EXTENDS = "extends";
  private static final String SPACE_ID = "spaceId";

  public ExtendedSpace(E event) throws SQLException, ErrorResponseException {
    super(event);
  }

  protected static <E extends Event> boolean isExtendedSpace(E event) {
    return event.getParams() != null && event.getParams().containsKey(EXTENDS);
  }

  protected static <E extends Event> boolean is2LevelExtendedSpace(E event) {
    return isExtendedSpace(event) && ((Map<String, Object>) event.getParams().get(EXTENDS)).containsKey(EXTENDS);
  }

  private static <E extends Event> String getFirstLevelExtendedTable(E event) {
    if (isExtendedSpace(event))
      return resolveExtendedTable(event, (Map<String, Object>) event.getParams().get(EXTENDS));
    return null;
  }

  private static <E extends Event> String getSecondLevelExtendedTable(E event) {
    if (is2LevelExtendedSpace(event)) {
      Map<String, Object> extSpec = (Map<String, Object>) event.getParams().get(EXTENDS);
      Map<String, Object> baseExtSpec = (Map<String, Object>) extSpec.get(EXTENDS);
      return resolveExtendedTable(event, baseExtSpec);
    }
    return null;
  }

  /**
   * Resolves the physical table name of an extended space.
   *
   * @param event The event that is being processed
   * @param extensionSpec The extension-spec that describes the extended space
   * @return The physical table name of the extended space
   */
  private static <E extends Event> String resolveExtendedTable(E event, Map<String, Object> extensionSpec) {
    if (extensionSpec == null)
      return null;
    if (extensionSpec.get(TABLE_NAME) instanceof String tableName && !tableName.isEmpty())
      return tableName;
    return XyzEventBasedQueryRunner.getTableNameForSpaceId(event, (String) extensionSpec.get(SPACE_ID));
  }

  public static <E extends Event> String getExtendedTable(E event) {
    if (is2LevelExtendedSpace(event))
      return getSecondLevelExtendedTable(event);
    else if (isExtendedSpace(event))
      return getFirstLevelExtendedTable(event);
    return null;
  }

  protected <E extends Event> String getIntermediateTable(E event) {
    if (is2LevelExtendedSpace(event))
      return getFirstLevelExtendedTable(event);
    return null;
  }
}
