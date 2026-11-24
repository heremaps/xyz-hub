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

package com.here.xyz.jobs.steps.impl.transport;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.jobs.steps.impl.transport.ExpressWriterDataSources.SCHEMA;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.impl.transport.TaskedSpaceBasedStep.SpaceBasedTaskUpdate;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput.ImportProgress;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput.WrittenFeatures;
import com.here.xyz.jobs.steps.impl.transport.tools.ImportQueryBuilder;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import org.junit.jupiter.api.Test;

/**
 * The stored output of an import task sums up what its batches have written.
 */
public class ImportTaskOutputMergeIT {

  private static final int TASK_ID = 1;

  @Test
  void sumsUpTheWrittenFeaturesOfAllBatchesOnce() throws Exception {
    ImportQueryBuilder queryBuilder = new ImportQueryBuilder(new Space().withVersionsToKeep(1), DEFAULT,
        (getClass().getSimpleName() + "_" + randomAlphabetic(5)).toLowerCase(), SCHEMA, "target", null, 37);

    try (DataSourceProvider dsp = ExpressWriterDataSources.getDataSourceProvider()) {
      queryBuilder.buildTaskTableStatement().write(dsp);
      try {
        new SQLQuery("INSERT INTO ${schema}.${table} (task_id, task_input) VALUES (#{taskId}, '{}'::JSONB)")
            .withVariable("schema", SCHEMA)
            .withVariable("table", queryBuilder.buildTaskTableStatement().getVariables().get("table"))
            .withNamedParameter("taskId", TASK_ID)
            .write(dsp);

        //Phase 1: the file was loaded into the staging table
        assertEquals(1, update(dsp, queryBuilder, new ImportOutput("5 rows imported", 100, 7)));
        assertNull(load(dsp, queryBuilder).written(), "No batch has written anything yet.");

        ImportOutput firstBatch = batch(1, 2, new WrittenFeatures(2, 0, 0, 2));
        assertEquals(1, update(dsp, queryBuilder, firstBatch));
        assertEquals(new WrittenFeatures(2, 0, 0, 2), load(dsp, queryBuilder).written());

        //A callback delivered twice is counted once
        assertEquals(0, update(dsp, queryBuilder, firstBatch));
        assertEquals(new WrittenFeatures(2, 0, 0, 2), load(dsp, queryBuilder).written());

        assertEquals(1, update(dsp, queryBuilder, batch(3, 5, new WrittenFeatures(1, 1, 1, 3))));

        ImportOutput stored = load(dsp, queryBuilder);
        assertEquals(new WrittenFeatures(3, 1, 1, 5), stored.written());
        assertEquals(5, stored.progress().endI());
        assertEquals("5 rows imported", stored.importStatistics(), "The phase 1 statistics must be kept.");
        assertEquals(5, stored.extractRowCount());

        //The final report has nothing written, the sum is kept
        //As sent by perform_import_from_tmp_table_task
        ImportOutput finalReport = XyzSerializable.deserialize(
            "{\"progress\":{\"type\":\"ImportOutput$ImportProgress\",\"tmpTableLoaded\":true},\"type\":\"ImportOutput\"}",
            new TypeReference<ImportOutput>() {});
        Boolean applied = queryBuilder.buildRetrieveTaskItemAndStatisticsAfterUpdateQuery(taskUpdate(finalReport))
            .run(dsp, rs -> rs.next() && rs.getBoolean("update_applied"));
        assertTrue(applied);
        ImportOutput finalized = load(dsp, queryBuilder);
        assertTrue(finalized.progress().tmpTableLoaded());
        assertEquals(new WrittenFeatures(3, 1, 1, 5), finalized.written());
        assertEquals("5 rows imported", finalized.importStatistics());
        assertEquals(100, finalized.fileBytes());
      }
      finally {
        queryBuilder.buildTemporaryJobTableDropStatement().write(dsp);
      }
    }
  }

  //Without fileBytes (0 is not serialized), as sent by the SQL
  private static ImportOutput batch(long startI, long endI, WrittenFeatures written) {
    return new ImportOutput(null, 0, 7, new ImportProgress(false, startI, endI), written);
  }

  private static int update(DataSourceProvider dsp, ImportQueryBuilder queryBuilder, ImportOutput output) throws Exception {
    return queryBuilder.buildUpdateTaskItemOutputStatement(taskUpdate(output)).write(dsp);
  }

  private static SpaceBasedTaskUpdate<ImportOutput> taskUpdate(ImportOutput output) {
    SpaceBasedTaskUpdate<ImportOutput> update = new SpaceBasedTaskUpdate<>();
    update.taskId = TASK_ID;
    update.taskOutput = output;
    return update;
  }

  private static ImportOutput load(DataSourceProvider dsp, ImportQueryBuilder queryBuilder) throws Exception {
    String taskOutput = queryBuilder.buildRetrieveTaskOutputsQuery().run(dsp, rs -> rs.next() ? rs.getString("task_output") : null);
    return XyzSerializable.deserialize(taskOutput, new TypeReference<ImportOutput>() {});
  }
}
