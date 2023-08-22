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
    createSpaceWithCustomStorage("x-psql-test-2", "psql", null);
    createSpaceWithExtension("x-psql-test");
    createSpaceWithExtension("x-psql-test-ext");

    touchSpaces();
  }

  public void touchSpaces() {
    //FIXME: in order to get the extending space to be created, a read or write operation must be executed, otherwise a 504 is returned
    getFeature("x-psql-test", "F1");
    getFeature("x-psql-test-2", "F1");
    getFeature("x-psql-test-ext", "F1");
    getFeature("x-psql-test-ext-ext", "F1");
  }

  @After
  public void tearDown() {
    removeSpace("x-psql-test-ext-ext");
    removeSpace("x-psql-test-ext");
    removeSpace("x-psql-test-2");
    removeSpace("x-psql-test");
  }
}
