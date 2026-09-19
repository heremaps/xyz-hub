/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import org.junit.After;
import org.junit.Before;

public class TestCompositeSpace extends TestSpaceWithFeature {

  @Before
  public void setup() {
    tearDown();
    createSpace();
    createSpaceWithCustomStorage(SECOND_SPACE_ID, "psql", null);
    createSpaceWithCustomStorage(THIRD_SPACE_ID, "psql_db2_hashed", null);
    createSpaceWithExtension(DEFAULT_SPACE_ID);
    createSpaceWithExtension(EXTENSION_SPACE_ID);

    touchSpaces();
  }

  public void touchSpaces() {
    //FIXME: in order to get the extending space to be created, a read or write operation must be executed, otherwise a 504 is returned
    getFeature(DEFAULT_SPACE_ID, "F1");
    getFeature(SECOND_SPACE_ID, "F1");
    getFeature(THIRD_SPACE_ID, "F1");
    getFeature(EXTENSION_SPACE_ID, "F1");
    getFeature(EXTENSION_EXTENSION_SPACE_ID, "F1");
  }

  @After
  public void tearDown() {
    removeSpace(EXTENSION_EXTENSION_SPACE_ID);
    removeSpace(EXTENSION_SPACE_ID);
    removeSpace(THIRD_SPACE_ID);
    removeSpace(SECOND_SPACE_ID);
    removeSpace(DEFAULT_SPACE_ID);
  }
}
