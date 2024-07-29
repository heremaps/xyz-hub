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

  //********************** Helper Functions *******************************/
  protected void writeFeature(List<Feature> modifiedFeatureList,
                              OnExists onExists, OnNotExists onNotExists,
                              OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                              SpaceContext spaceContext, boolean isHistoryActive, SQLErrorCodes expectedError)
          throws Exception {
    //second write on existing feature
    runWriteFeatureQueryWithSQLAssertion(modifiedFeatureList, author, onExists , onNotExists,
            onVersionConflict, onMergeConflict, isPartial, spaceContext, isHistoryActive, expectedError);
  }

  protected void createAndUpdateFeature(Long baseVersion, boolean hasConflictingAttributes,
                                        OnExists onExists, OnNotExists onNotExists,
                                        OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial,
                                        SpaceContext spaceContext, boolean isHistoryActive, SQLErrorCodes expectedError)
          throws Exception {

    //initial Insert
    runWriteFeatureQueryWithSQLAssertion(Arrays.asList(createTestFeature(null)), author, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    List<Feature> modifiedFeatureList = baseVersion != null ? Arrays.asList(createModifiedTestFeature(baseVersion, hasConflictingAttributes))
            : Arrays.asList(createModifiedTestFeature(null,false));

    writeFeature(modifiedFeatureList, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial,
            spaceContext, isHistoryActive, expectedError);
  }

  protected Feature createTestFeature(Long baseVersion) {
    Feature feature = new Feature()
              .withId("id1")
              .withProperties(new Properties().with("name", "t1"))
              .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));

    if(baseVersion != null)
      feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(baseVersion));

    return feature;
  }

  protected Feature createModifiedTestFeature(Long baseVersion, boolean hasConflictingAttributes) {
    Feature feature = new Feature()
            .withId("id1")
            .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));
    if(hasConflictingAttributes){
      feature.setProperties(new Properties()
              //"name" is already present in initial write
              .with("name", "modified")
              //"foo" is new
              .with("foo", "bar2")
      );
    }else{
      feature.setProperties(new Properties()
              //Both keys are new changes
              .with("foo", "bar")
              .with("new", "value")
      );
    }

    //add a version which a user would provide
    if(baseVersion != null)
      feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(baseVersion));

    return feature;
  }

  protected Feature createMergedTestFeature(Long baseVersion) {
    Feature feature = new Feature()
            .withId("id1")
            .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)));
    feature.setProperties(new Properties()
            .with("foo", "bar")
            .with("new", "value")
            .with("name", "t1"));

    feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(baseVersion));

    return feature;
  }
}
