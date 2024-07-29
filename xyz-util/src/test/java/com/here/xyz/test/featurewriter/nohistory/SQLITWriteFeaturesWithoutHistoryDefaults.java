/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.test.featurewriter.nohistory;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.test.featurewriter.SQLITWriteFeaturesBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SQLITWriteFeaturesWithoutHistoryDefaults extends SQLITWriteFeaturesBase {

  @Test
  public void writeFeatureWithDefaults() throws Exception {
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    checkExistingFeature(createSimpleTestFeature(), 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  @Test
  public void writeExistingFeatureWithDefaults() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(null,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, null,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeature(null,false), 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  @Test
  public void writeExistingFeatureWithDefaultsPartial() throws Exception {
    //Insert Feature
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //second write with partial modifications
    writeFeature(Arrays.asList(createPartialTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, true, SpaceContext.EXTENSION,false, null);

    checkExistingFeature(createPartialTestFeatureResult(), 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }
}
