/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.psql.query.branching;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.here.xyz.models.hub.Ref;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BMBase extends QueryTestBase {

  protected static final int MAIN_NODE_ID = 0;
  protected static final String REF_MAIN = "~" + MAIN_NODE_ID;
  protected static final Ref REF_MAIN_HEAD = new Ref(REF_MAIN + ":HEAD");

  @BeforeEach
  public void prepare() throws Exception {
    cleanup();
    createSpaceTable(PG_SCHEMA, spaceId());
  }

  @AfterEach
  public void cleanup() throws Exception {
    dropSpaceTables(PG_SCHEMA, spaceId());
  }

  protected final String spaceId() {
    return getClass().getSimpleName();
  }

  protected Ref createBranch(Ref baseRef) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      return branchManager(dsp).createBranch(baseRef);
    }
  }

  protected void assertFeatureExists(String id, Ref ref) throws Exception {
    assertNotNull(readFeatureById(id, ref), "Feature " + id + " should exist in ref " + ref);
  }

  protected void assertFeatureNotExists(String id, Ref ref) throws Exception {
    assertNull(readFeatureById(id, ref), "Feature " + id + " should not exist in ref " + ref);
  }
}
