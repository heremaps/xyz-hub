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

import static com.here.xyz.events.ModifySubscriptionEvent.Operation.CREATE;
import static com.here.xyz.events.ModifySubscriptionEvent.Operation.DELETE;
import static com.here.xyz.events.ModifySubscriptionEvent.Operation.UPDATE;
import static com.here.xyz.psql.query.ModifySpace.SPACE_META_TABLE;
import static com.here.xyz.responses.XyzError.EXCEPTION;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ModifySubscription extends XyzQueryRunner<ModifySubscriptionEvent, FeatureCollection> {

  public ModifySubscription(ModifySubscriptionEvent event)
      throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  protected SQLQuery buildQuery(ModifySubscriptionEvent event) throws SQLException, ErrorResponseException {
    SQLQuery query;
    if (event.getOperation() == CREATE || event.getOperation() == UPDATE)
      query = new SQLQuery("INSERT INTO ${{spaceMetaTable}} (id, schem, h_id, meta) "
          + "VALUES (#{spaceId}, #{schema}, #{table}, #{metaValue}::JSONB) "
          + "ON CONFLICT (id, schem) DO "
          + "UPDATE SET meta = ${{spaceMetaTable}}.meta || excluded.meta")
          .withNamedParameter(TABLE, getDefaultTable(event))
          .withNamedParameter("metaValue", "{\"subscriptions\":true}");
    else if (event.getOperation() == DELETE)
      query = new SQLQuery("UPDATE ${{spaceMetaTable}} SET meta = meta - 'subscriptions' WHERE (id, schem) = (#{spaceId}, #{schema})");
    else
      throw new ErrorResponseException(EXCEPTION, "Unsupported operation for ModifySubscriptionEvent: " + event.getOperation());

    return query
        .withQueryFragment("spaceMetaTable", SPACE_META_TABLE)
        .withNamedParameter("spaceId", event.getSpace())
        .withNamedParameter(SCHEMA, getSchema());
  }

  @Override
  public FeatureCollection handle(ResultSet rs) throws SQLException {
    return null;
  }
}
