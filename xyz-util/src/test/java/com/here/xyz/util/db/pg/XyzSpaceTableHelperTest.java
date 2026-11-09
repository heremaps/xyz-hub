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

package com.here.xyz.util.db.pg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.db.ConnectorParameters.TableLayout;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class XyzSpaceTableHelperTest {

  @ParameterizedTest
  @EnumSource(TableLayout.class)
  void spaceTablesKeepVersioningWithoutCreatingBranchSequences(TableLayout layout) {
    String sql = XyzSpaceTableHelper.buildCreateSpaceTableQueries("public", "test-table", "logical-space", layout).stream()
        .map(query -> query.substitute().text())
        .collect(Collectors.joining("\n"));

    assertTrue(sql.contains("test-table_version_seq"), sql);
    assertTrue(sql.contains("xyz_create_history_partition"), sql);
    assertFalse(sql.contains("branches_seq"), sql);
  }
}
