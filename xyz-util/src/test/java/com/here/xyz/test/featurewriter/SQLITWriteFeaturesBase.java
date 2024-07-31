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

package com.here.xyz.test.featurewriter;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.test.SQLITSpaceBase;

import java.util.Arrays;
import java.util.List;

public class SQLITWriteFeaturesBase extends SQLITSpaceBase {
  //********************** Helper Functions *******************************/
  protected void writeFeature(Feature modifiedFeature, String author,
                              OnExists onExists, OnNotExists onNotExists,
                              OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                              SpaceContext spaceContext, boolean isHistoryActive, SQLError expectedError)
          throws Exception {
    writeFeatures(Arrays.asList(modifiedFeature), author, onExists , onNotExists,
            onVersionConflict, onMergeConflict, isPartial, spaceContext, isHistoryActive, expectedError);
  }

  protected void writeFeatures(List<Feature> modifiedFeatureList, String author,
                              OnExists onExists, OnNotExists onNotExists,
                              OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                              SpaceContext spaceContext, boolean isHistoryActive, SQLError expectedError)
          throws Exception {
    runWriteFeatureQueryWithSQLAssertion(modifiedFeatureList, author, onExists , onNotExists,
            onVersionConflict, onMergeConflict, isPartial, spaceContext, isHistoryActive, expectedError);
  }

  protected void performMerge(Feature feature1, Feature feature2, Feature expected, OnMergeConflict onMergeConflict,
                              SQLError expectedError) throws Exception {
    //initial write
    writeFeature(feature1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Lead into a version conflict, because version 0 is not present anymore
    writeFeature(feature2, UPDATE_AUTHOR,null,null, OnVersionConflict.MERGE, onMergeConflict,
            false, SpaceContext.EXTENSION, false, expectedError );
  }
}
