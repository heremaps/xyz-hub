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

public class SQLITWriteFeaturesWithoutHistoryFeatureExists extends SQLITWriteFeaturesBase {

  //********************** Feature exists (OnVersionConflict deactivated) *******************************/
  @Test
  public void writeToExistingFeature_OnExistsDELETE() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createModifiedTestFeature(null,false)), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(null,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, OnExists.DELETE,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(DEFAULT_FEATURE_ID);
  }

  @Test
  public void writeToExistingFeature_OnExistsREPLACE() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(null,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, OnExists.REPLACE,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeature(null,false), 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_OnExistsRETAIN() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(null,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, OnExists.RETAIN,null,null,null,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(createSimpleTestFeature(), 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_OnExistsERROR() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(null,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, OnExists.ERROR,null,null,null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ44);

    checkExistingFeature(createSimpleTestFeature(), 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** Feature exists (OnVersionConflict.REPLACE + BaseVersion Match) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsDELETE() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(1L,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, OnExists.DELETE,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(DEFAULT_FEATURE_ID);
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsREPLACE() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(1L,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, OnExists.REPLACE,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeature(null,false), 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsRETAIN() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(1L,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR,OnExists.RETAIN,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createSimpleTestFeature(), 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsERROR() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(1L,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, OnExists.ERROR,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ44);

    checkExistingFeature(createSimpleTestFeature(), 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** Feature exists + BaseVersion Conflict (onVersionConflict.ERROR) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictERROR() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(2L,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, null,null, OnVersionConflict.ERROR, null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ49);
    checkExistingFeature(createSimpleTestFeature(), 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** Feature exists + BaseVersion Conflict (onVersionConflict.RETAIN) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictRETAIN() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(2L,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, null,null, OnVersionConflict.RETAIN, null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createSimpleTestFeature(), 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** Feature exists + BaseVersion Conflict (OnVersionConflict.REPLACE) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictREPLACE() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(1L,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, null,null, OnVersionConflict.REPLACE, null,
            false, SpaceContext.EXTENSION, false, null);

    //Third write with modifications
    modifiedFeatureList = Arrays.asList(createModifiedTestFeature(1L, false));

    //Lead into a version conflict, because version 1 is not present anymore
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR,null,null, OnVersionConflict.REPLACE, null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeature(null, false), 3L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  //********************** Feature exists + BaseVersion Conflict + no merge conflict (OnVersionConflict.MERGE) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictMERGE() throws Exception {
    //initial write
    writeFeature(Arrays.asList(createSimpleTestFeature()), DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    List<Feature> modifiedFeatureList = Arrays.asList(createModifiedTestFeature(1L,false));
    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR, null,null, OnVersionConflict.REPLACE, null,
            false, SpaceContext.EXTENSION, false, null);

    //Lead into a version conflict, because version 1 is not present anymore
    //We have no conflicting versions! But we are able to merge.
    modifiedFeatureList = Arrays.asList(createModifiedTestFeature(1L, false));

    writeFeature(modifiedFeatureList, DEFAULT_AUTHOR,null,null, OnVersionConflict.MERGE, null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeature(null, false), 3L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }
}
