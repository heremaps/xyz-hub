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
import com.here.xyz.responses.SuccessResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteChangesets extends XyzQueryRunner<DeleteChangesetsEvent, SuccessResponse> {
  private static final Logger logger = LogManager.getLogger();
  private DeleteChangesetsEvent event;

  public DeleteChangesets(DeleteChangesetsEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
    this.event = event;
  }

  @Override
  public SuccessResponse run() throws SQLException, ErrorResponseException {
    long headVersion = new GetHeadVersion<>(event, dbHandler).run();
    if(event.getMinTagVersion() != null && event.getMinTagVersion() < event.getMinVersion())
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Tag for version " + event.getMinTagVersion() +" exists!");
    if (event.getMinVersion() > headVersion)
      throw new ErrorResponseException(ILLEGAL_ARGUMENT, "Can not delete all changesets older than version " + event.getMinVersion()
          + " as it would also delete the HEAD (" + headVersion
          + ") version. Minimum version which may specified as new minimum version is HEAD.");
    return super.run();
  }

  @Override
  protected SQLQuery buildQuery(DeleteChangesetsEvent event) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT xyz_delete_changesets(#{schema}, #{table}, #{partitionSize}, #{minVersion})")
        .withNamedParameter(SCHEMA, getSchema())
        .withNamedParameter(TABLE, getDefaultTable(event))
        .withNamedParameter("partitionSize", PARTITION_SIZE)
        .withNamedParameter("minVersion", calculateMinVersion(event))
        .withAsync(true);
  }

  @Override
  public SuccessResponse handle(ResultSet rs) throws SQLException {
    return new SuccessResponse();
  }

  private long calculateMinVersion(DeleteChangesetsEvent event){
    if(event.getMinTagVersion() != null && event.getMinTagVersion() < event.getMinVersion()){
      /** In this case cant delete some parts of the requested version because the version would
       * further be needed form the notification producer. If it is possible to delete parts,
       * the minVersion (stored in the space-definition) will reflect the user-choice. So it will
       * not be possible to request changesets which are potentially and temporary present.*/
      logger.info("Reduce minVersion:"+event.getMinVersion()+" -> "+event.getMinTagVersion());
      return event.getMinTagVersion();
    }
    return event.getMinVersion();
  }
}
