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
import com.here.xyz.test.featurewriter.SQLITWriteFeaturesBase;
import org.junit.Test;

public class SQLITWriteFeaturesWithoutHistoryFeatureExists extends SQLITWriteFeaturesBase {

  //********************** Feature exists (OnVersionConflict deactivated) *******************************/
  @Test
  public void writeToExistingFeature_OnExistsDELETE() throws Exception {
    createAndUpdateFeature(OnExists.DELETE,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(createModifiedTestFeatures().get(0));
  }

  @Test
  public void writeToExistingFeature_OnExistsREPLACE() throws Exception {
    createAndUpdateFeature(OnExists.REPLACE,null,null,null,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(createModifiedTestFeatures().get(0), 2L, Long.MAX_VALUE, Operation.U, author);
  }

  @Test
  public void writeToExistingFeature_OnExistsRETAIN() throws Exception {
    createAndUpdateFeature(OnExists.RETAIN,null,null,null,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(createTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToExistingFeature_OnExistsERROR() throws Exception {
    createAndUpdateFeature(OnExists.ERROR,null,null,null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ44);
    checkExistingFeature(createTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  //********************** Feature exists (OnVersionConflict.REPLACE + BaseVersion Match) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsDELETE() throws Exception {
    createAndUpdateFeature(1L,false, OnExists.DELETE,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(createTestFeatures().get(0));
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsREPLACE() throws Exception {
    createAndUpdateFeature(1L,false, OnExists.REPLACE,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(createModifiedTestFeatures().get(0), 2L, Long.MAX_VALUE, Operation.U, author);
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsRETAIN() throws Exception {
    createAndUpdateFeature(1L,false, OnExists.RETAIN,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(createTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsERROR() throws Exception {
    createAndUpdateFeature(1L,false, OnExists.ERROR,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ44);
    checkExistingFeature(createTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  //********************** Feature exists + BaseVersion Conflict (onVersionConflict.ERROR) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictERROR() throws Exception {
    createAndUpdateFeature(2L,false, null,null, OnVersionConflict.ERROR, null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ49);
    checkExistingFeature(createTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  //********************** Feature exists + BaseVersion Conflict (onVersionConflict.RETAIN) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictRETAIN() throws Exception {
    createAndUpdateFeature(2L,false,null,null, OnVersionConflict.RETAIN, null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  //********************** Feature exists + BaseVersion Conflict (OnVersionConflict.REPLACE) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictREPLACE() throws Exception {
    createAndUpdateFeature(1L,false,null,null, OnVersionConflict.REPLACE, null,
            false, SpaceContext.EXTENSION, false, null);

    //Lead into a version conflict, because version 1 is not present anymore
    writeFeature(1L,false,null,null, OnVersionConflict.REPLACE, null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeatures().get(0), 3L, Long.MAX_VALUE, Operation.U, author);
  }

  //********************** Feature exists + BaseVersion Conflict + no merge conflict (OnVersionConflict.MERGE) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictMERGE() throws Exception {
    createAndUpdateFeature(1L,false,null,null, OnVersionConflict.REPLACE, null,
            true, SpaceContext.EXTENSION, false, null);

    //Lead into a version conflict, because version 1 is not present anymore
    //We have no conflicting changes!
    writeFeature(1L,false,null,null, OnVersionConflict.MERGE, null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createMergedTestFeatures(1L).get(0), 3L, Long.MAX_VALUE, Operation.U, author);
  }
}
