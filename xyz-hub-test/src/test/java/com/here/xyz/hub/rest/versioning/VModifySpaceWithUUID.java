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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.hub.rest.ModifySpaceWithUUID;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import org.junit.After;
import org.junit.Before;

public class VModifySpaceWithUUID extends ModifySpaceWithUUID {

  @Before
  public void setup() {
    String spaceId = getSpaceId();
    removeSpace(spaceId);
    VersioningBaseIT.createSpace(spaceId, getSpacesPath(), 10, true);
    addFeatures(spaceId);
  }

  @Override
  protected FeatureCollection prepareUpdateWithUnexistingUUID() throws JsonProcessingException {
    FeatureCollection fc = super.prepareUpdateWithUnexistingUUID();
    fc.getFeatures().get(0).getProperties().getXyzNamespace().setVersion(9999);
    return fc;
  }

  @Override
  protected FeatureCollection prepareUpdateWithNullUUID() throws JsonProcessingException {
    FeatureCollection fc = super.prepareUpdateWithNullUUID();
    fc.getFeatures().get(0).getProperties().getXyzNamespace().setVersion(-1);
    return fc;
  }

  @Override
  public void testPatchWithHistory() throws JsonProcessingException {}

  @Override
  public void testMergeWithHistory() throws JsonProcessingException {}

  @Override
  public void testPatchConflictWithHistory() throws JsonProcessingException {}
}
