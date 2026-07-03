/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static com.here.xyz.models.hub.Ref.HEAD;

import com.here.xyz.FeatureChange.Operation;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.IterateChangesetsBuilder.IterateChangesetsInput;
import com.here.xyz.util.db.SQLQuery;
import java.util.Map;

public class IterateChangesetsBuilder extends XyzQueryBuilder<IterateChangesetsInput>{


  @Override
  protected SQLQuery buildQuery(IterateChangesetsInput input) throws QueryBuildingException {
    /*
    TODO: Provide the possibility to specify the following via the input:
    - the parts of the page token separately & in plain form
    - the desired operation (e.g. inserted, updated, deleted) to be returned
     */

    //TODO: Remove that workaround when refactoring is complete
    IterateChangesetsEvent event = new IterateChangesetsEvent()
        .withSpace(input.spaceId)
        .withConnectorParams(input.connectorParams)
        .withParams(input.spaceParams)
        .withVersionsToKeep(input.versionsToKeep)
        .withContext(input.context)
        .withRef(input.ref)
        .withMinVersion(input.minVersion)
        .withStartTime(input.startTime)
        .withEndTime(input.endTime)
        .withOperation(input.operation)
        .withSquashed(input.squashed);

    //TODO: Use nextPageToken once it supports also an upper I-bound instead of a limit


    event.ignoreLimit = true;

    return null;
  }

  public record IterateChangesetsInput(
      String spaceId,
      Map<String, Object> connectorParams,
      Map<String, Object> spaceParams,
      SpaceContext context,
      int versionsToKeep,
      long minVersion,
      Ref ref,

      long startTime, //optional instead of ref
      long endTime, //optional instead of ref
      boolean squashed,

      Operation operation //Optional: Only return changes with the specified operation
  ) {
    public IterateChangesetsInput {
      if (ref == null)
        ref = new Ref(HEAD);
    }
  }
}
