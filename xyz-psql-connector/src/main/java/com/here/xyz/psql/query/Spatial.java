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

import static com.here.xyz.events.GetFeaturesByTileEvent.ResponseType.GEO_JSON;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.SpatialQueryEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.SQLQueryBuilder;
import java.sql.SQLException;

public abstract class Spatial<E extends SpatialQueryEvent> extends SearchForFeatures<E> {

  public Spatial(E event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  protected static SQLQuery buildClippedGeoFragment(final SpatialQueryEvent event) {
    boolean convertToGeoJson = !(event instanceof GetFeaturesByTileEvent && ((GetFeaturesByTileEvent) event).getResponseType() != GEO_JSON);
    String forceMode = SQLQueryBuilder.getForceMode(event.isForce2D());

    if (!event.getClip())
      return new SQLQuery(convertToGeoJson
          ? "replace(ST_AsGeojson(" + forceMode + "(geo), " + SQLQueryBuilder.GEOMETRY_DECIMAL_DIGITS + "),'nan','0') as geo"
          : forceMode + "(geo) as geo");

    return null; //Should never happen
  }
}
