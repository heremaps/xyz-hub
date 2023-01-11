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

import static com.here.xyz.psql.DatabaseHandler.PARTITION_SIZE;
import static com.here.xyz.responses.XyzError.ILLEGAL_ARGUMENT;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.helpers.versioning.GetHeadVersion;
import com.here.xyz.psql.query.helpers.versioning.GetOldestPartitionRangeMin;
import com.here.xyz.responses.SuccessResponse;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteChangesets extends XyzQueryRunner<DeleteChangesetsEvent, SuccessResponse> {

  private DeleteChangesetsEvent event;

  public DeleteChangesets(DeleteChangesetsEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
    this.event = event;
  }

  @Override
  public void write() throws SQLException, ErrorResponseException {
    long headVersion = new GetHeadVersion<>(event, dbHandler).run();
    if (event.getMinVersion() > headVersion)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Can not delete all changesets older than version " + event.getMinVersion()
          + " as it would also delete the HEAD (" + headVersion
          + ") version. Minimum version which may specified as new minimum version is HEAD.");
    super.write();
  }

  @Override
  protected SQLQuery buildQuery(DeleteChangesetsEvent event) throws SQLException, ErrorResponseException {
    //TODO: Execute asynchronously in DB
    return new SQLQuery("${{removePartitions}}${{purgeRemainder}}")
        .withQueryFragment("removePartitions", buildRemovePartitionsQuery(event))
        .withQueryFragment("purgeRemainder", buildPurgeRemainderFromOldestPartition(event))
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, getDefaultTable(event));
  }

  protected SQLQuery buildRemovePartitionsQuery(DeleteChangesetsEvent event) throws SQLException, ErrorResponseException {
    String table = getDefaultTable(event);
    long previousMinRangeMin = new GetOldestPartitionRangeMin(event, dbHandler).run();
    String sql = "";
    for (long partitionNo = previousMinRangeMin / PARTITION_SIZE; partitionNo < (event.getMinVersion() + 1) / PARTITION_SIZE; partitionNo++)
      sql += "ALTER TABLE IF EXISTS ${schema}.${table} DETACH PARTITION \"" + table + "_p" + partitionNo + "\" CONCURRENTLY;"
          + "DROP TABLE IF EXISTS ${schema}.\"" + table + "_p" + partitionNo + "\";";
    return new SQLQuery(sql);
  }

  protected SQLQuery buildPurgeRemainderFromOldestPartition(DeleteChangesetsEvent event) {
    return new SQLQuery("DELETE FROM ${schema}.${table} WHERE next_version <= #{minVersion}")
        .withNamedParameter("minVersion", event.getMinVersion());
  }

  @Override
  public SuccessResponse handle(ResultSet rs) throws SQLException {
    return null;
  }
}
