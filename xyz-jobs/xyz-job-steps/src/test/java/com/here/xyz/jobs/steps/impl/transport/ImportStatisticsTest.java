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

import static com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.withWrittenFeatures;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Public;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput.ImportProgress;
import com.here.xyz.jobs.steps.impl.transport.tasks.outputs.ImportOutput.WrittenFeatures;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The statistics of an import: The requested features (rows in the input files) next to how they were written.
 */
class ImportStatisticsTest {

  private static final ImportProgress LOADED = new ImportProgress(true);

  @Test
  void countsAllRowsOfAnImportIntoAnEmptyLayerAsInserted() {
    FeatureStatistics statistics = withWrittenFeatures(new FeatureStatistics().withFeatureCount(8), List.of(
        new ImportOutput("5 rows imported", 100, 7),
        new ImportOutput("3 rows imported", 60, 7)));

    assertCounts(statistics, 8, 0, 0, 0);
  }

  @Test
  void sumsUpTheWrittenFeaturesOfAllTasks() {
    FeatureStatistics statistics = withWrittenFeatures(new FeatureStatistics().withFeatureCount(10), List.of(
        new ImportOutput("6 rows imported", 100, 7, LOADED, new WrittenFeatures(2, 1, 1)),
        new ImportOutput("4 rows imported", 60, 7, LOADED, new WrittenFeatures(1, 2, 0))));

    //10 requested, 7 written => 3 unchanged
    assertCounts(statistics, 3, 3, 1, 3);
  }

  @Test
  void countsAnEmptyFileAsNothingWritten() {
    //A file without any feature has no batch, so it reports nothing
    FeatureStatistics statistics = withWrittenFeatures(new FeatureStatistics().withFeatureCount(4), List.of(
        new ImportOutput("4 rows imported", 100, 7, LOADED, new WrittenFeatures(0, 1, 0)),
        new ImportOutput("0 rows imported", 0, 7, LOADED, null)));

    assertCounts(statistics, 0, 1, 0, 3);
  }

  @Test
  void leavesTheCountsNullIfATaskWithRowsDidNotReportThem() {
    //E.g., a task executed by a previous version of the import, which does not report what it has written yet
    FeatureStatistics statistics = withWrittenFeatures(new FeatureStatistics().withFeatureCount(10), List.of(
        new ImportOutput("6 rows imported", 100, 7, LOADED, new WrittenFeatures(6, 0, 0)),
        new ImportOutput("4 rows imported", 60, 7, LOADED, null)));

    assertNull(statistics.getInsertedCount());
    assertNull(statistics.getUpdatedCount());
    assertNull(statistics.getDeletedCount());
    assertNull(statistics.getUnchangedCount());
  }

  /**
   * The written counts are only used for the logs for now, so neither the stored output nor the job API contains them.
   */
  @Test
  void doesNotSerializeTheWrittenCounts() {
    FeatureStatistics statistics = withWrittenFeatures(new FeatureStatistics().withFeatureCount(3).withByteSize(10),
        List.of(new ImportOutput("3 rows imported", 10, 7, LOADED, new WrittenFeatures(1, 1, 0))));

    for (String json : List.of(XyzSerializable.serialize(statistics), XyzSerializable.serialize(statistics, Public.class),
        XyzSerializable.serialize(statistics, Static.class))) {
      assertTrue(json.contains("\"featureCount\":3"), json);
      for (String field : List.of("insertedCount", "updatedCount", "deletedCount", "unchangedCount"))
        assertFalse(json.contains(field), json);
    }
  }

  private static void assertCounts(FeatureStatistics statistics, long inserted, long updated, long deleted, long unchanged) {
    assertEquals(inserted, statistics.getInsertedCount(), "inserted");
    assertEquals(updated, statistics.getUpdatedCount(), "updated");
    assertEquals(deleted, statistics.getDeletedCount(), "deleted");
    assertEquals(unchanged, statistics.getUnchangedCount(), "unchanged");
  }
}
