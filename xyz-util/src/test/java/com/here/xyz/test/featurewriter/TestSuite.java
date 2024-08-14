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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.test.GenericSpaceBased;
import com.here.xyz.test.GenericSpaceBased.OnExists;
import com.here.xyz.test.GenericSpaceBased.OnMergeConflict;
import com.here.xyz.test.GenericSpaceBased.OnNotExists;
import com.here.xyz.test.GenericSpaceBased.OnVersionConflict;
import com.here.xyz.test.GenericSpaceBased.Operation;
import com.here.xyz.test.GenericSpaceBased.SQLError;
import org.junit.After;
import org.junit.Before;

public abstract class TestSuite {

  protected GenericSpaceBased genericSpaceWriter;

  protected String testName;
  protected boolean composite;
  protected boolean history;
  protected boolean featureExists;
  protected Boolean baseVersionMatch; //TODO: Use unboxed type
  protected Boolean conflictingAttributes; //TODO: Use unboxed type
  protected Boolean featureExistsInSuper; //TODO: Use unboxed type
  protected Boolean featureExistsInExtension; //TODO: Use unboxed type
  protected UserIntent userIntent;
  protected OnNotExists onNotExists;
  protected OnExists onExists;
  protected OnVersionConflict onVersionConflict;
  protected OnMergeConflict onMergeConflict;
  protected SpaceContext spaceContext;
  protected Expectations expectations;

  public TestSuite(TestArgs args) {
    this.testName = args.testName;
    this.composite = args.composite;
    this.history = args.history;
    this.featureExists = args.featureExists;
    this.baseVersionMatch = args.baseVersionMatch;
    this.conflictingAttributes = args.conflictingAttributes;
    this.featureExistsInSuper = args.featureExistsInSuper;
    this.featureExistsInExtension = args.featureExistsInExtension;

    this.userIntent = args.userIntent;
    this.onNotExists = args.onNotExists;
    this.onExists = args.onExists;
    this.onVersionConflict = args.onVersionConflict;
    this.onMergeConflict = args.onMergeConflict;
    this.spaceContext = args.spaceContext;

    this.expectations = args.expectations;

    if (this.spaceContext == null)
      this.spaceContext = EXTENSION; //TODO: Check if that default makes sense
  }

  protected static Feature featureWithEmptyProperties() {
    return new Feature().withId("id1").withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)))
        .withProperties(new Properties());
  }

  protected static Feature simpleFeature() throws JsonProcessingException {
    return featureWithEmptyProperties().withProperties(new Properties().with("firstName", "Alice").with("age", 35));
  }

  protected static Feature simple1stModifiedFeature() throws JsonProcessingException {
    return simple1stModifiedFeature(-1);
  }

  protected static Feature simple1stModifiedFeature(long version) throws JsonProcessingException {
    Feature feature = simpleFeature();
    feature.getProperties().with("lastName", "Wonder");

    if (version != -1)
      feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(version));

    return feature;
  }

  protected static Feature simple2ndModifiedFeature(Long version, Boolean conflictingAttributes) throws JsonProcessingException {
    Feature feature = featureWithEmptyProperties();
    feature.getProperties().with("age", "32");

    if (conflictingAttributes != null && conflictingAttributes)
      feature.getProperties().with("lastName", "NotWonder");

    if (version != null)
      feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(version));

    return feature;
  }

  @Before
  public void prepare() throws Exception {
    genericSpaceWriter.createSpaceResources();
  }

  @After
  public void clean() throws Exception {
    genericSpaceWriter.cleanSpaceResources();
  }

  public void featureWriterExecutor() throws Exception {
    if (baseVersionMatch != null)
      featureWriterExecutor_WithBaseVersion();
    else
      featureWriterExecutor_WithoutBaseVersion();
  }

  public void featureWriterExecutor_WithoutBaseVersion() throws Exception {
    //TODO: Check UserIntent

    if (!this.featureExists)
      genericSpaceWriter.writeFeature(simpleFeature(), GenericSpaceBased.DEFAULT_AUTHOR, onExists, onNotExists, onVersionConflict,
          onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
    else {
      //Simple 1th write
      genericSpaceWriter.writeFeature(simpleFeature(), GenericSpaceBased.DEFAULT_AUTHOR, null, null, null, null, false, EXTENSION, history,
          null);
      //2th write
      genericSpaceWriter.writeFeature(simple1stModifiedFeature(), GenericSpaceBased.UPDATE_AUTHOR, onExists, onNotExists, onVersionConflict,
          onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
    }
    checkStrategies();
  }

  public void featureWriterExecutor_WithBaseVersion() throws Exception {
    //TODO: Check UserIntent

    if (!this.featureExists)
      genericSpaceWriter.writeFeature(simpleFeature(), GenericSpaceBased.DEFAULT_AUTHOR, onExists, onNotExists, onVersionConflict,
          onMergeConflict, false, spaceContext, history, expectations != null ? expectations.sqlError() : null);
    else {
      //Simple 1th write
      genericSpaceWriter.writeFeature(simpleFeature(), GenericSpaceBased.DEFAULT_AUTHOR, null, null, null, null, false, EXTENSION, history,
          null);
      //Simple 2th write
      genericSpaceWriter.writeFeature(simple1stModifiedFeature(1), GenericSpaceBased.DEFAULT_AUTHOR, null, null, null, null, false,
          EXTENSION, history, null);
      //3th write
      Long version = baseVersionMatch != null ? (baseVersionMatch ? 2L : 1L) : null;
      if (baseVersionMatch != null)
        genericSpaceWriter.writeFeature(simple2ndModifiedFeature(version, conflictingAttributes), GenericSpaceBased.UPDATE_AUTHOR, onExists,
            onNotExists, onVersionConflict, onMergeConflict, false, spaceContext, history,
            expectations != null ? expectations.sqlError() : null);
    }
    checkStrategies();
  }

  private void checkStrategies() throws Exception {
    if (onExists != null)
      switch (onExists) {
        case ERROR, RETAIN -> checkRetainOrError();
        case DELETE -> checkOnExistsDelete();
        case REPLACE -> checkFeaturesOnReplace();
      }

    if (onNotExists != null)
      switch (onNotExists) {
        case ERROR, RETAIN -> checkRetainOrError();
        case CREATE -> checkOnNotExistsCreate();
      }

    if (onVersionConflict != null)
      switch (onVersionConflict) {
        case ERROR, RETAIN -> checkRetainOrError();
        case MERGE -> checkFeaturesOnMerge();
        case REPLACE -> {
          //Has priority
          if (onExists != null)
            break;
          checkFeaturesOnReplace();
        }
      }

    if (onMergeConflict != null)
      switch (onMergeConflict) {
        case ERROR, RETAIN -> checkRetainOrError();
        case REPLACE -> checkFeaturesOnReplace();
      }
  }

  private void checkOnNotExistsCreate() throws Exception {
    genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
        expectations.featureOperation(), expectations.author());
    genericSpaceWriter.checkFeatureCount(1);
  }

  private void checkOnExistsDelete() throws Exception {
    if (history) {
      genericSpaceWriter.checkDeletedFeatureOnHistory(simpleFeature().getId(), true);

      if (baseVersionMatch != null)
        //1th insert, 2th update, 3th delete
        genericSpaceWriter.checkFeatureCount(3);
      else
        //1th insert, 2th delete
        genericSpaceWriter.checkFeatureCount(2);
    }
    else {
      genericSpaceWriter.checkNotExistingFeature(simpleFeature().getId());
      //Feature should not exist in the table
      genericSpaceWriter.checkFeatureCount(0);
    }
  }

  private void checkRetainOrError() throws Exception {
    if (this.featureExists)
      genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
          expectations.featureOperation(), expectations.author());
    else
      genericSpaceWriter.checkNotExistingFeature(simpleFeature().getId());

    checkFeatureCountsForRetainOrError();
  }

  private void checkFeaturesOnReplace() throws Exception {
    if (history) {
      //First Write
      genericSpaceWriter.checkExistingFeature(simpleFeature(), 1L, 2L, GenericSpaceBased.Operation.I, GenericSpaceBased.DEFAULT_AUTHOR);
      //Last Write
      genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
          expectations.featureOperation(), expectations.author());
    }
    else
      //Head
      genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
          expectations.featureOperation(), expectations.author());

    checkFeatureCountsForReplace();
  }

  private void checkFeaturesOnMerge() throws Exception {
    //In our merge tests we have at least one feature
    if (history) {
      //First Write
      genericSpaceWriter.checkExistingFeature(simpleFeature(), 1L, 2L, GenericSpaceBased.Operation.I, GenericSpaceBased.DEFAULT_AUTHOR);
      //Last Write
      genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
          expectations.featureOperation(), expectations.author());
      if (onMergeConflict != null && (onMergeConflict.equals(GenericSpaceBased.OnMergeConflict.ERROR) || onMergeConflict.equals(
          GenericSpaceBased.OnMergeConflict.RETAIN))) {
        //1th insert / 2th update
        genericSpaceWriter.checkFeatureCount(2);
      }
      else
        //1th insert / 2th update / 3th merge
        genericSpaceWriter.checkFeatureCount(3);
    }
    else {
      //Head
      genericSpaceWriter.checkExistingFeature(expectations.feature(), expectations.version(), expectations.nextVersion(),
          expectations.featureOperation(), expectations.author());
      genericSpaceWriter.checkFeatureCount(1);
    }
  }

  /**
   * Count checkers
   */
  private void checkFeatureCountsForReplace() throws Exception {
    if (history) {
      if (baseVersionMatch != null)
        //1th insert, 2th update, 3th Update
        genericSpaceWriter.checkFeatureCount(3);
      else
        //1th insert, 2th update
        genericSpaceWriter.checkFeatureCount(2);
    }
    else
      //Head
      genericSpaceWriter.checkFeatureCount(1);
  }

  private void checkFeatureCountsForRetainOrError() throws Exception {
    if (this.featureExists) {
      if (history) {
        if (baseVersionMatch != null)
          //1th insert, 2th update
          genericSpaceWriter.checkFeatureCount(2);
        else
          //1th insert
          genericSpaceWriter.checkFeatureCount(1);
      }
      else
        //Feature should exist in the table
        genericSpaceWriter.checkFeatureCount(1);
    }
    else {
      if (history) {
        if (baseVersionMatch != null)
          //1th insert
          genericSpaceWriter.checkFeatureCount(1);
        else
          //Feature should not exist in the table
          genericSpaceWriter.checkFeatureCount(0);
      }
      else
        //Feature should not exist in the table
        genericSpaceWriter.checkFeatureCount(0);
    }
  }

  public enum TableOperation {
    INSERT,
    UPDATE,
    DELETE
  }

  protected enum UserIntent {
    WRITE, //Illegal Argument
    DELETE
  }

  //TODO: Use unboxed type instead of Booleans
  public record TestArgs(String testName, boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch,
      Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension, UserIntent userIntent,
      GenericSpaceBased.OnNotExists onNotExists, GenericSpaceBased.OnExists onExists, GenericSpaceBased.OnVersionConflict onVersionConflict,
      GenericSpaceBased.OnMergeConflict onMergeConflict, SpaceContext spaceContext, Expectations expectations) {

    public TestArgs withComposite(boolean composite) {
      return new TestArgs(testName, true, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper,
          featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
    }

    public TestArgs withContext(SpaceContext spaceContext) {
      return new TestArgs(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper,
          featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, expectations);
    }

    @Override
    public String toString() {
      return testName;
    }
  }

  public record Expectations(TableOperation tableOperation, Operation featureOperation, Feature feature, long version, long nextVersion,
      String author, SQLError sqlError) {

    public Expectations(SQLError sqlError) {
      this(null, null, null, 0L, 0L, null, sqlError);
    }

    public Expectations(TableOperation tableOperation) {
      this(tableOperation, null, null, 0L, 0L, null, null);
    }
  }
}
