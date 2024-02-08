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

import static com.here.xyz.psql.query.ModifySpace.SPACE_META_TABLE_FQN;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

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
    if(event.getMinTagVersion() != null && event.getMinTagVersion() < event.getRequestedMinVersion())
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Tag for version " + event.getMinTagVersion() +" exists!");
    if (event.getRequestedMinVersion() > headVersion)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Can not delete all changesets older than version " + event.getRequestedMinVersion()
              + " as it would also delete the HEAD (" + headVersion
              + ") version. Minimum version which may specified as new minimum version is HEAD.");

    return super.write(dataSourceProvider);
  }

  @Override
  protected SuccessResponse handleWrite(int[] rowCounts) throws ErrorResponseException {
    if (rowCounts[0] == 0)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Version < '"+event.getRequestedMinVersion()+"' is already deleted!");
    return new SuccessResponse();
  }

  @Override
  protected SQLQuery buildQuery(DeleteChangesetsEvent event) throws SQLException, ErrorResponseException {
    /** Update "userMinVersion" which flags the minimum Version the user wants to have. The deletion will happen asynchronously. */
    return new SQLQuery("UPDATE "+ SPACE_META_TABLE_FQN +" " +
            "SET meta = meta || #{userMinVersionJson}::jsonb " +
            "WHERE id=#{spaceId} " +
            " AND (meta->'userMinVersion' < #{userMinVersion}::text::jsonb OR meta->'userMinVersion' IS NULL) " +
            " AND COALESCE((meta->'minAvailableVersion')::bigint, 0) < #{userMinVersion}")
            .withNamedParameter("userMinVersionJson", "{\"userMinVersion\":"+event.getRequestedMinVersion()+"}")
            .withNamedParameter("userMinVersion",event.getRequestedMinVersion())
            .withNamedParameter("spaceId", event.getSpace());
  }

  @Override
  public SuccessResponse handle(ResultSet rs) throws SQLException {
    return new SuccessResponse();
  }
}
