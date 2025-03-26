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

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.GetFeaturesByIdsBuilder.GetFeaturesByIdsInput;
import com.here.xyz.util.db.SQLQuery;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class GetFeaturesByIdsBuilder extends XyzQueryBuilder<GetFeaturesByIdsInput> {
  private SQLQuery selectClauseOverride;

  public GetFeaturesByIdsBuilder withSelectClauseOverride(SQLQuery selectClauseOverride) {
    this.selectClauseOverride = selectClauseOverride;
    return this;
  }

  @Override
  public SQLQuery buildQuery(GetFeaturesByIdsInput input) throws QueryBuildingException {
    try {
      //TODO: Remove that workaround when refactoring is complete
      GetFeaturesByIdEvent event = new GetFeaturesByIdEvent()
          .withSpace(input.spaceId)
          .withConnectorParams(input.connectorParams)
          .withParams(input.spaceParams)
          .withVersionsToKeep(input.versionsToKeep)
          .withContext(input.context)
          .withRef(input.ref)
          .withIds(input.ids);

      return new GetFeaturesByIdWithModifiedFilter(event)
          .<GetFeaturesById>withDataSourceProvider(getDataSourceProvider())
          .buildQuery(event);
    }
    catch (SQLException | ErrorResponseException e) {
      throw new QueryBuildingException(e);
    }
  }

  public record GetFeaturesByIdsInput(
      String spaceId,
      Map<String, Object> connectorParams,
      Map<String, Object> spaceParams,
      SpaceContext context,
      int versionsToKeep,
      Ref ref,
      List<String> ids
  ) {
    public GetFeaturesByIdsInput {
      if (ref == null)
        ref = new Ref(Ref.HEAD);
    }
  }

  private class GetFeaturesByIdWithModifiedFilter extends GetFeaturesById {

    public GetFeaturesByIdWithModifiedFilter(GetFeaturesByIdEvent event) throws SQLException, ErrorResponseException {
      super(event);
    }

    @Override
    protected SQLQuery buildSelectClause(GetFeaturesByIdEvent event, int dataset) {
      return overrideSelectClause(super.buildSelectClause(event, dataset), selectClauseOverride);
    }

    private SQLQuery overrideSelectClause(SQLQuery selectClause, SQLQuery selectClauseOverride) {
      if (selectClauseOverride != null)
        return new SQLQuery("${{selectClause}}")
                .withQueryFragment("selectClause", selectClauseOverride);
      return selectClause;
    }
  }
}
