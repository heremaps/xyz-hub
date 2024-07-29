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
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.test.SQLITSpaceBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SQLITWriteFeaturesBase extends SQLITSpaceBase {

  @Test
  public void writeFeaturesWithDefaults() throws Exception {
    insertTestFeature(null,null,null);
    checkExistingFeature(createTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeExistingFeatureWithDefaults() throws Exception {
    createAndUpdateFeature(null,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeatures().get(0), 2L, Long.MAX_VALUE, Operation.U, author);
  }

  //********************** Helper Functions *******************************/
  protected void createAndUpdateFeature(OnExists onExists, OnNotExists onNotExists,
                                        OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                                        SpaceContext spaceContext, boolean isHistoryActive, SQLErrorCodes expectedError) throws Exception {
    createAndUpdateFeature(null, false, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial,
            spaceContext, isHistoryActive, expectedError);
  }

  protected void createAndUpdateFeature(Long baseVersion, boolean hasConflictingAttributes,
                                        OnExists onExists, OnNotExists onNotExists,
                                        OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                                        SpaceContext spaceContext, boolean isHistoryActive, SQLErrorCodes expectedError)
          throws Exception {

    //initial Insert
    insertTestFeature(null, null,null);
    writeFeature(baseVersion, hasConflictingAttributes, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial,
            spaceContext, isHistoryActive, expectedError);
  }

  protected void writeFeature(Long baseVersion, boolean hasConflictingAttributes,
                                      OnExists onExists, OnNotExists onNotExists,
                                      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                                      SpaceContext spaceContext, boolean isHistoryActive, SQLErrorCodes expectedError)
          throws Exception {

    //TODO impl baseVersionMatch / hasConflictingAttributes
    //modify properties
    List<Feature> modifiedFeatureList = baseVersion != null ? createModifiedTestFeatures(baseVersion, hasConflictingAttributes) : createModifiedTestFeatures();

    //second write on existing feature
    runWriteFeatureQueryWithSQLAssertion(modifiedFeatureList, author, onExists , onNotExists,
            onVersionConflict, onMergeConflict, isPartial, spaceContext, isHistoryActive, expectedError);
  }

  protected void insertTestFeature( OnExists onExists, OnNotExists onNotExists, SQLErrorCodes expectedError)
          throws Exception {

    runWriteFeatureQueryWithSQLAssertion(createTestFeatures(), author, onExists , onNotExists,
            null, null, false, SpaceContext.EXTENSION,false, expectedError);
  }

  protected List<Feature> createTestFeatures() {
    return Arrays.asList(
            new Feature()
                    .withId("id1")
                    .withProperties(new Properties().with("name", "t1"))
                    .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)))
    );
  }

  protected List<Feature> createModifiedTestFeatures() {
    return createModifiedTestFeatures(false);
  }

  protected List<Feature> createModifiedTestFeatures(boolean hasConflictingAttributes) {
    return createModifiedTestFeatures(null, hasConflictingAttributes);
  }

  private List<Feature> createModifiedTestFeatures(Long baseVersion, boolean hasConflictingAttributes) {
    Feature feature = new Feature()
            .withId("id1")
            .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));
    if(hasConflictingAttributes){
      feature.setProperties(new Properties()
              .with("name", "modified")
              .with("foo", "bar2")
      );
    }else{
      feature.setProperties(new Properties()
              .with("foo", "bar")
              .with("new", "value")
      );
    }
    if(baseVersion != null)
      feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(baseVersion));

    return Arrays.asList(feature);
  }

  protected List<Feature> createMergedTestFeatures(Long baseVersion) {
    Feature feature = new Feature()
            .withId("id1")
            .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));
    feature.setProperties(new Properties()
            .with("foo", "bar")
            .with("new", "value")
            .with("name", "t1"));
    feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(baseVersion));
    return Arrays.asList(feature);
  }
}
