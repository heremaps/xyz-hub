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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.models.hub.Space;
import org.junit.jupiter.api.Test;

class ImportQueryBuilderTest {

  @Test
  void buildsExpressQueryForCompositeDefaultContext() {
    ImportQueryBuilder queryBuilder = new ImportQueryBuilder(
        new Space().withVersionsToKeep(10),
        DEFAULT,
        "step",
        "xyz",
        "extension_table",
        "super_table",
        37
    );

    String sql = queryBuilder.buildExpressImportFromTmpTableTaskQuery(
        1, 1, "owner", 2, true, "{}", "lambda", "region", "PERFORM 1;"
    ).substitute().text();

    assertTrue(sql.contains("perform_express_import_from_tmp_table_task"));
    assertTrue(sql.contains("'xyz.\"extension_table\"'"));
    assertTrue(sql.contains("'xyz.\"super_table\"'"));
    assertTrue(sql.contains("'DEFAULT'"));
  }

  @Test
  void targetsSuperTableForSuperContext() {
    ImportQueryBuilder queryBuilder = new ImportQueryBuilder(
        new Space().withVersionsToKeep(10),
        SUPER,
        "step",
        "xyz",
        "extension_table",
        "super_table",
        37
    );

    String sql = queryBuilder.buildExpressImportFromTmpTableTaskQuery(
        1, 1, "owner", 2, true, "{}", "lambda", "region", "PERFORM 1;"
    ).substitute().text();

    assertTrue(sql.contains("'xyz.\"super_table\"'"));
    assertTrue(sql.contains("'SUPER'"));
  }

  @Test
  void allocatesVersionsFromExplicitTargetTable() {
    ImportQueryBuilder queryBuilder = new ImportQueryBuilder(
        new Space().withVersionsToKeep(10),
        SUPER,
        "step",
        "xyz",
        "extension_table",
        "super_table",
        37
    );

    assertTrue(queryBuilder.buildNextVersionQuery("super_table")
        .substitute().text().contains("\"super_table_version_seq\""));
  }
}
