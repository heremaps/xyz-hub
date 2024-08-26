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

package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import java.sql.SQLException;
import java.util.Map;

public abstract class XyzEventBasedQueryRunner<E extends Event, R extends Object> extends QueryRunner<E, R> {
  private boolean preferPrimaryDataSource;

  public XyzEventBasedQueryRunner(E event) throws SQLException, ErrorResponseException {
    super(event);
    preferPrimaryDataSource = event.getPreferPrimaryDataSource();
  }

  public static String readTableFromEvent(Event event) {
    Map<String, Object> spaceParams = event != null ? event.getParams() : null;
    boolean hashed = ConnectorParameters.fromEvent(event).isEnableHashedSpaceId();

    String spaceId = null;
    if (event != null && event.getSpace() != null && event.getSpace().length() > 0)
      spaceId = event.getSpace();

    return XyzSpaceTableHelper.getTableNameFromSpaceParamsOrSpaceId(spaceParams, spaceId, hashed);
  }

  protected static String getTableNameForSpaceId(Event event, String spaceId) {
    return XyzSpaceTableHelper.getTableNameForSpaceId(spaceId, ConnectorParameters.fromEvent(event).isEnableHashedSpaceId());
  }

  protected String getDefaultTable(E event) {
    return readTableFromEvent(event);
  }

  @Override
  public boolean isUseReadReplica() {
    //Always use the writer in case of event.preferPrimaryDataSource == true
    return super.isUseReadReplica() && !preferPrimaryDataSource;
  }
}
