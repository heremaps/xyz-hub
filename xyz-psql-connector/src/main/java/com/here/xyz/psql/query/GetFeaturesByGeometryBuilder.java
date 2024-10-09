/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.GetFeaturesByGeometryBuilder.GetFeaturesByGeometryInput;
import com.here.xyz.util.db.SQLQuery;
import java.sql.SQLException;

public class GetFeaturesByGeometryBuilder extends XyzQueryBuilder<GetFeaturesByGeometryInput> {
  @Override
  public SQLQuery buildQuery(GetFeaturesByGeometryInput input) throws QueryBuildingException {
    try {
      //TODO: Remove that workaround when refactoring is complete
      GetFeaturesByGeometryEvent event = new GetFeaturesByGeometryEvent()
          .withSpace(input.spaceId)
          .withParams(XyzSerializable.toMap(getConnectorParameters()))
          .withVersionsToKeep(input.versionsToKeep)
          .withContext(input.context)
          .withRef(input.ref)
          .withPropertiesQuery(input.propertiesQuery)
          .withGeometry(input.geometry)
          .withRadius(input.radius)
          .withClip(input.clip);

      return new GetFeaturesByGeometry(event)
          .<GetFeaturesByGeometry>withDataSourceProvider(getDataSourceProvider())
          .buildQuery(event);
    }
    catch (SQLException | ErrorResponseException e) {
      throw new QueryBuildingException(e);
    }
  }

  public record GetFeaturesByGeometryInput(
      String spaceId,
      SpaceContext context,
      int versionsToKeep,
      Ref ref,
      Geometry geometry,
      int radius,
      boolean clip,
      PropertiesQuery propertiesQuery
  ) {}
}
