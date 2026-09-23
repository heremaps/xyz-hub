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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

class ExpressImportSqlDefinitionTest {

  @Test
  void expressWriterProcessesRowsWithoutFeatureCollectionAggregation() throws IOException {
    try (InputStream stream = getClass().getResourceAsStream("/jobs/transport.sql")) {
      assertNotNull(stream);
      String transportSql = new String(stream.readAllBytes(), UTF_8);
      String expressWriter = transportSql.substring(
          transportSql.indexOf("CREATE OR REPLACE FUNCTION execute_express_import_batch"),
          transportSql.indexOf("CREATE OR REPLACE FUNCTION perform_express_import_from_tmp_table_task")
      );

      assertTrue(expressWriter.contains("FOR source_row IN"));
      assertTrue(expressWriter.contains("pulled_count > 0 AND pulled_bytes + row_bytes > target_bytes"));
      assertTrue(expressWriter.contains("DISTINCT ON (feature_id)"));
      assertFalse(expressWriter.contains("jsonb_agg"));
      assertFalse(expressWriter.contains("write_features("));
    }
  }
}
