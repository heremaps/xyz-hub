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

package com.here.xyz.psql.query.helpers;

import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class GetIndexListTest {

  @Test
  void indexListComesFromTheFirstTableThatIsNotTheTemplate() {
    assertEquals("own", GetIndexList.indexListTable("own", List.of()));
    assertEquals("own", GetIndexList.indexListTable("own", List.of("intermediate", "root")));
    assertEquals("intermediate", GetIndexList.indexListTable(TEMPLATE_TABLE, List.of("intermediate", "root")));
    assertEquals("root", GetIndexList.indexListTable(TEMPLATE_TABLE, List.of(TEMPLATE_TABLE, "root")));
    assertEquals("root", GetIndexList.indexListTable(TEMPLATE_TABLE, List.of("root")));
    assertEquals(TEMPLATE_TABLE, GetIndexList.indexListTable(TEMPLATE_TABLE, List.of(TEMPLATE_TABLE, TEMPLATE_TABLE)));
    assertEquals(TEMPLATE_TABLE, GetIndexList.indexListTable(TEMPLATE_TABLE, List.of()));
  }
}
