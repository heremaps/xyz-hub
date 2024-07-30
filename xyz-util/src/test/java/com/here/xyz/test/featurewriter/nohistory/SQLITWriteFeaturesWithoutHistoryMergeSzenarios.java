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

public class SQLITWriteFeaturesWithoutHistoryMergeSzenarios extends SQLITWriteFeaturesBase {

  //********************** Feature exists + BaseVersion Conflict + merge conflict (OnVersionConflict.MERGE) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictMERGE_With_MergeConflict_OnMergeConflictDefault() throws Exception {
    //Default is ERROR
    //initial write
    writeFeature(createSimpleTestFeature(), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    Feature f = createModifiedTestFeature(1L, false);

    //Second partial write with modifications
    writeFeature(f, DEFAULT_AUTHOR, null,null, null, null,
            true, SpaceContext.EXTENSION, false, null);
    //Lead into a version conflict, because version 1 is not present anymore
    //We have conflicting changes!

    f = createModifiedTestFeature(1L, true);
    writeFeature(f, DEFAULT_AUTHOR,null,null, OnVersionConflict.MERGE, OnMergeConflict.ERROR,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ48);
    checkExistingFeature(createMergedTestFeatureResult(), 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

    @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictMERGE_With_MergeConflict_OnMergeReplace() throws Exception {
    //initial write
    writeFeature(createSimpleTestFeature(), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second partial write with modifications
    Feature f = createModifiedTestFeature(1L, false);
    writeFeature(f, DEFAULT_AUTHOR, null,null, null,null,
            true, SpaceContext.EXTENSION, false, null);

    //Lead into a version conflict, because version 1 is not present anymore
    //We have conflicting changes!
    f = createModifiedTestFeature(1L, true);

    writeFeature(f, DEFAULT_AUTHOR,null,null, OnVersionConflict.MERGE, OnMergeConflict.REPLACE,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(f, 3L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictMERGE_With_MergeConflict_OnMergeConflictError() throws Exception {
    //initial write
    writeFeature(createSimpleTestFeature(), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second partial write with modifications
    Feature f = createModifiedTestFeature(1L,false);
    writeFeature(f, DEFAULT_AUTHOR, null,null, null, null,
            true, SpaceContext.EXTENSION, false, null);
    //Lead into a version conflict, because version 1 is not present anymore
    //We have conflicting changes!
    f = createModifiedTestFeature(1L, true);

    writeFeature(f, DEFAULT_AUTHOR,null,null, OnVersionConflict.MERGE, OnMergeConflict.ERROR,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ48);
    checkExistingFeature(createMergedTestFeatureResult(), 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }
}
