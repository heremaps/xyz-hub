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

package com.here.xyz.test;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SQLITWriteFeatures extends SQLITSpaceBase{
  private static String author = "testAuthor";

  @Test
  public void writeFeaturesWithDefaults() throws Exception {
    insertTestFeature(null,null,null);
    checkExistingFeature(createTestFeature().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToExistingFeatureWithDefaults_WOHistory() throws Exception {
    writeToExistingFeature(null,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.U, author);
  }

  //********************** Feature not exists *******************************/
  @Test
  public void writeToNotExistingFeature_OnNotExistsCREATE_WOHistory() throws Exception {
    insertTestFeature(null, OnNotExists.CREATE, null);

    checkExistingFeature(createTestFeature().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToNotExistingFeature_OnNotExistsERROR_WOHistory() throws Exception {
    insertTestFeature(null,OnNotExists.CREATE, SQLErrorCodes.XYZ40);
  }

  @Test
  public void writeToNotExistingFeature_OnNotExistsRETAIN_WOHistory() throws Exception {
    insertTestFeature(null, OnNotExists.RETAIN,null);
    checkNotExistingFeature(createTestFeature().get(0));
  }

  //********************** Feature exists *******************************/
  @Test
  public void writeToExistingFeature_OnExistsDELETE_WOHistory() throws Exception {
    writeToExistingFeature(OnExists.DELETE,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(createModifiedTestFeature().get(0));
  }

  @Test
  public void writeToExistingFeature_OnExistsREPLACE_WOHistory() throws Exception {
    writeToExistingFeature(OnExists.REPLACE,null,null,null,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.U, author);
  }

  @Test
  public void writeToExistingFeature_OnExistsRETAIN_WOHistory() throws Exception {
    writeToExistingFeature(OnExists.RETAIN,null,null,null,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(createModifiedTestFeature().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToExistingFeature_OnExistsERROR_WOHistory() throws Exception {
    writeToExistingFeature(OnExists.ERROR,null,null,null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ44);
    checkExistingFeature(createModifiedTestFeature().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  //********************** Feature exists + BaseVersion Match + OnVersionConflict.REPLACE *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersionMatch_OnExistsDELETE_WOHistory() throws Exception {
    writeToExistingFeature(1L,false, OnExists.DELETE,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(createModifiedTestFeature().get(0));
  }

//
//      @Test
//      public void writeToExistingFeature_WithBaseVersionMatch_OnExistsREPLACE_WOHistory() throws Exception {
//        writeToExistingFeature(1L,false, OnExists.REPLACE,null, OnVersionConflict.REPLACE,null,
//                false, SpaceContext.EXTENSION, false, null);
//        checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.U, author);
//      }
//
//      @Test
//      public void writeToExistingFeature_WithBaseVersionMatch_OnExistsRETAIN_WOHistory() throws Exception {
//        writeToExistingFeature(1L,false, OnExists.RETAIN,null, OnVersionConflict.REPLACE,null,
//                false, SpaceContext.EXTENSION, false, null);
//        checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.I, author);
//      }
//
//      @Test
//      public void writeToExistingFeature_WithBaseVersionMatch_OnExistsERROR_WOHistory() throws Exception {
//        writeToExistingFeature(1L,false, OnExists.ERROR,null, OnVersionConflict.REPLACE,null,
//                false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ44);
//        checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.I, author);
//      }

  //********************** Feature exists + BaseVersion Conflict + OnVersionConflict.ERROR *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersionConflict_OnVersionConflictERROR_WOHistory() throws Exception {
    writeToExistingFeature(2L,false, null,null, OnVersionConflict.ERROR, null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ49);
    checkExistingFeature(createModifiedTestFeature().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  //********************** Feature exists + BaseVersion Conflict + OnVersionConflict.REPLACE + VersionConflict Handling *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersionConflict_OnMergeConflictERROR_WOHistory() throws Exception {
    writeToExistingFeature(2L,false,null,null, OnVersionConflict.REPLACE, OnMergeConflict.ERROR,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ49);

    checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.U, author);
  }
//
//  @Test
//  public void writeToExistingFeature_WithBaseVersionConflict_OnMergeConflictRETAIN_WOHistory() throws Exception {
//    writeToExistingFeature(2L,false, null,null, OnVersionConflict.REPLACE, OnMergeConflict.RETAIN,
//            false, SpaceContext.EXTENSION, false, null);
//    checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.U, author);
//  }
//
//  @Test
//  public void writeToExistingFeature_WithBaseVersionConflict_OnMergeConflictREPLACE_WOHistory() throws Exception {
//    writeToExistingFeature(2L,false, null,null, OnVersionConflict.REPLACE, OnMergeConflict.REPLACE,
//            false, SpaceContext.EXTENSION, false, null);
//    checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.I, author);
//  }
//
//  @Test
//  public void writeToExistingFeature_WithBaseVersionConflict_OnMergeConflictMERGE_WOHistory() throws Exception {
//    writeToExistingFeature(2L,false, OnExists.ERROR,null, OnVersionConflict.REPLACE, OnMergeConflict.MERGE,
//            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ44);
//    checkExistingFeature(createModifiedTestFeature().get(0), 2L, Long.MAX_VALUE, Operation.I, author);
//  }

  //********************** Helper Functions *******************************/
  private void writeToExistingFeature(OnExists onExists, OnNotExists onNotExists,
                                       OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                                       SpaceContext spaceContext, boolean isHistoryActive, SQLErrorCodes expectedError) throws Exception {
    writeToExistingFeature(null, false, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial,
            spaceContext, isHistoryActive, expectedError);
  }

  private void writeToExistingFeature(Long baseVersion, boolean hasConflictingAttributes,
                                           OnExists onExists, OnNotExists onNotExists,
                                           OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                                           SpaceContext spaceContext, boolean isHistoryActive, SQLErrorCodes expectedError)
          throws Exception {

    //initial Insert
    insertTestFeature(onExists, onNotExists,null);

    //TODO impl baseVersionMatch / hasConflictingAttributes
    //modify properties
    List<Feature> modifiedFeatureList = baseVersion != null ? createModifiedTestFeature(baseVersion) : createModifiedTestFeature();

    //second write on existing feature
    runWriteFeatureQueryWithSQLAssertion(modifiedFeatureList, author, onExists , onNotExists,
            onVersionConflict, onMergeConflict, isPartial, spaceContext, hasConflictingAttributes, expectedError);
  }

  private void insertTestFeature( OnExists onExists, OnNotExists onNotExists, SQLErrorCodes expectedError)
          throws Exception {

    runWriteFeatureQueryWithSQLAssertion(createTestFeature(), author, onExists , onNotExists,
            null, null, false, SpaceContext.EXTENSION,false, expectedError);
  }

  private List<Feature> createTestFeature() {
    return Arrays.asList(
            new Feature()
                    .withId("id1")
                    .withProperties(new Properties().with("name", "t1"))
                    .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)))
    );
  }

  private List<Feature> createModifiedTestFeature() {
    return createModifiedTestFeature(null);
  }

  private List<Feature> createModifiedTestFeature(Long baseVersion) {
    Feature feature = new Feature()
            .withId("id1")
            .withProperties(new Properties().with("name", "modified").with("foo", "bar"))
            .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));
    if(baseVersion != null)
      feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(baseVersion));
    return Arrays.asList(feature);
  }
}
