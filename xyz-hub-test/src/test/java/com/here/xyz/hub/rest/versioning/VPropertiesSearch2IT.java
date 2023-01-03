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

import com.here.xyz.hub.rest.PropertiesSearch2IT;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class VPropertiesSearch2IT extends PropertiesSearch2IT {

  @BeforeClass
  public static void setup() {
    String spaceId = getSpaceId();
    removeSpace(spaceId);
    VersioningBaseIT.createSpace(spaceId, getSpacesPath(), 10);
    addFeatures();
  }
  @AfterClass
  public static void tearDown() {
    VersioningBaseIT.tearDown();
  }
}
