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
package com.here.xyz.jobs.steps.impl.transport.tools;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

public abstract class DatabaseStepQueryBuilder {
  private final Space space;
  private final ContextAwareEvent.SpaceContext context;
  private final String superRootTable;

  protected static final String JOB_DATA_PREFIX = "job_data_";
  protected final String stepId;
  protected final String schema;
  protected final String rootTable;

  protected DatabaseStepQueryBuilder(Space space, ContextAwareEvent.SpaceContext context, String stepId, String schema,
                                     String rootTable, String superRootTable) {
    this.stepId = stepId;
    this.schema = schema;

    this.space = space;
    this.context = context;

    this.rootTable = rootTable;
    this.superRootTable = superRootTable;
  }

  protected String getTemporaryJobTableName() {
    return JOB_DATA_PREFIX + stepId;
  }

  @JsonIgnore
  protected Map<String, Object> getQueryContext(){
    List<String> tables = new ArrayList<>();
    if (superRootTable != null)
      tables.add(superRootTable);
    tables.add(rootTable);

    return new FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder()
            .withSchema(schema)
            .withTables(tables)
            //Honor a user-provided space context (e.g. EXTENSION) and fall back to DEFAULT if none was set
            .withSpaceContext(context != null ? context : DEFAULT)
            .withHistoryEnabled(space.getVersionsToKeep() > 1)
            .withBatchMode(true)
            .with("stepId", stepId)
            .build();
  }
}