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
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.responses.XyzResponse;
import java.sql.SQLException;
import java.util.Map;

public abstract class ExtendedSpace<E extends Event, R extends XyzResponse> extends QueryRunner<E, R> {

  private static final String EXTENDS = "extends";
  private static final String SPACE_ID = "spaceId";

  public ExtendedSpace(E event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  protected boolean isExtendedSpace(E event) {
    return event.getParams() != null && event.getParams().containsKey(EXTENDS);
  }

  protected boolean is2LevelExtendedSpace(E event) {
    return isExtendedSpace(event) && ((Map<String, Object>) event.getParams().get(EXTENDS)).containsKey(EXTENDS);
  }

  private String getFirstLevelExtendedTable(E event) {
    if (isExtendedSpace(event))
      return dbHandler.getConfig().getTableNameForSpaceId((String) ((Map<String, Object>) event.getParams().get(EXTENDS)).get(SPACE_ID));
    return null;
  }

  private String getSecondLevelExtendedTable(E event) {
    if (is2LevelExtendedSpace(event)) {
      Map<String, Object> extSpec = (Map<String, Object>) event.getParams().get(EXTENDS);
      Map<String, Object> baseExtSpec = (Map<String, Object>) extSpec.get(EXTENDS);
      return dbHandler.getConfig().getTableNameForSpaceId((String) baseExtSpec.get(SPACE_ID));
    }
    return null;
  }

  protected String getExtendedTable(E event) {
    if (is2LevelExtendedSpace(event))
      return getSecondLevelExtendedTable(event);
    else if (isExtendedSpace(event))
      return getFirstLevelExtendedTable(event);
    return null;
  }

  protected String getIntermediateTable(E event) {
    if (is2LevelExtendedSpace(event))
      return getFirstLevelExtendedTable(event);
    return null;
  }
}
