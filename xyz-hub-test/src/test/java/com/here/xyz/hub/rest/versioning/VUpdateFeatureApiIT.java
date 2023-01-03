/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub.rest.versioning;

import com.here.xyz.hub.rest.UpdateFeatureApiIT;
import org.junit.After;
import org.junit.Before;

public class VUpdateFeatureApiIT extends UpdateFeatureApiIT {

  @Before
  public void setup() {
    VersioningBaseIT.setup();
  }
  @After
  public void tearDown() {
    VersioningBaseIT.tearDown();
  }
}
