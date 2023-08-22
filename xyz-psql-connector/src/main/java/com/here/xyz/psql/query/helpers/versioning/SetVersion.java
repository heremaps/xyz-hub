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

package com.here.xyz.psql.query.helpers.versioning;

import static com.here.xyz.psql.query.helpers.versioning.GetNextVersion.VERSION_SEQUENCE_SUFFIX;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.psql.query.helpers.versioning.SetVersion.SetVersionInput;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SetVersion extends QueryRunner<SetVersionInput, Void> {

  public SetVersion(SetVersionInput input) throws SQLException, ErrorResponseException {
    super(input);
  }

  @Override
  protected SQLQuery buildQuery(SetVersionInput input) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT setval('${schema}.${sequence}', #{version}, true)")
        .withVariable(SCHEMA, getSchema())
        .withVariable("sequence", XyzEventBasedQueryRunner.readTableFromEvent(input.event) + VERSION_SEQUENCE_SUFFIX)
        .withNamedParameter("version", input.version);
  }

  @Override
  public Void handle(ResultSet rs) throws SQLException {
    return null;
  }

  public static class SetVersionInput {
    private ModifyFeaturesEvent event;
    private long version;

    public SetVersionInput(ModifyFeaturesEvent event, long version) {
      this.event = event;
      this.version = version;
    }
  }
}
