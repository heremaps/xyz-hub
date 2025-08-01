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

import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.PARTITION_SIZE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.psql.query.helpers.versioning.GetHeadVersion;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteChangesets extends XyzQueryRunner<DeleteChangesetsEvent, SuccessResponse> {
  private DeleteChangesetsEvent event;

  public DeleteChangesets(DeleteChangesetsEvent event) throws SQLException, ErrorResponseException {
    super(event);
    this.event = event;
  }

  @Override
  public SuccessResponse write(DataSourceProvider dataSourceProvider) throws SQLException, ErrorResponseException {
    long headVersion = new GetHeadVersion<>(event).withDataSourceProvider(dataSourceProvider).run();
    if (event.getMinVersion() > headVersion)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Can not delete all changesets older than version " + event.getMinVersion()
              + " as it would also delete the HEAD (" + headVersion
              + ") version. Minimum version which may specified as new minimum version is HEAD.");

    return super.write(dataSourceProvider);
  }

  @Override
  protected SuccessResponse handleWrite(int[] rowCounts) throws ErrorResponseException {
    if (rowCounts[0] == 0)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Version < '"+event.getMinVersion()+"' is already deleted!");
    return new SuccessResponse();
  }

  @Override
  protected SQLQuery buildQuery(DeleteChangesetsEvent event) throws SQLException, ErrorResponseException {

    return new SQLQuery("SELECT xyz_delete_changesets(#{schema}, #{table}, #{partitionSize}, #{minVersion})")
            .withNamedParameter("schema", getSchema())
            .withNamedParameter("table", getDefaultTable(event))
            .withNamedParameter("partitionSize", PARTITION_SIZE)
            //TODO: check NTF
            //minVersion=Math.min(requestedVersion, space.minTagVersion)
            .withNamedParameter("minVersion", event.getMinVersion())
            .withAsync(true);
  }

  @Override
  public SuccessResponse handle(ResultSet rs) throws SQLException {
    return new SuccessResponse();
  }
}
