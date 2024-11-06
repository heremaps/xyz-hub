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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.test.featurewriter.SpaceWriter.DEFAULT_AUTHOR;
import static com.here.xyz.test.featurewriter.SpaceWriter.OTHER_AUTHOR;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.D;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.H;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.I;
import static com.here.xyz.test.featurewriter.SpaceWriter.Operation.J;
import static com.here.xyz.test.featurewriter.SpaceWriter.UPDATE_AUTHOR;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.DELETE;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.INSERT;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.NONE;
import static com.here.xyz.test.featurewriter.TestSuite.TableOperation.UPDATE;
import static com.here.xyz.util.db.pg.SQLError.UNKNOWN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.test.featurewriter.SpaceWriter.Operation;
import com.here.xyz.test.featurewriter.sql.SQLSpaceWriter;
import com.here.xyz.util.db.pg.SQLError;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class TestSuite {
  public static final String TEST_FEATURE_ID = "id1";
  public static final Point TEST_FEATURE_GEOMETRY = new Point().withCoordinates(new PointCoordinates(8, 50));
  private static final TestArgs EMPTY_ARGS = new TestArgs("EMPTY_ARGS", false, false, false, false, false, false, UserIntent.WRITE, null, null, null, null, null, null);

  protected String testName;
  protected boolean composite;
  protected boolean history;
  protected boolean featureExists;
  protected boolean baseVersionMatch;
  protected boolean conflictingAttributes;
  protected boolean featureExistsInSuper;
  protected boolean featureExistsInExtension;
  protected UserIntent userIntent;
  protected OnNotExists onNotExists;
  protected OnExists onExists;
  protected OnVersionConflict onVersionConflict;
  protected OnMergeConflict onMergeConflict;
  protected SpaceContext spaceContext;
  protected Map<SpaceContext, LongAdder> writtenSpaceVersions = new HashMap<>(Map.of(
      SUPER, new LongAdder(),
      EXTENSION, new LongAdder()
  ));
  protected long baseVersion;

  protected SpaceState beforeState;
  protected SQLError thrownError;
  protected SpaceState afterState;
  protected TestAssertions assertions;
  protected boolean applyAuthorWorkaround = false;

  static {
    XyzSerializable.setAlwaysSerializePretty(true);
  }

  protected abstract SpaceWriter spaceWriter();

  protected TestArgs modifyArgs(TestArgs args) {
    return args;
  }

  private void init(TestArgs args) {
    testName = args.testName;
    composite = args.composite;
    history = args.history;
    featureExists = args.featureExists;
    baseVersionMatch = args.baseVersionMatch;
    conflictingAttributes = args.conflictingAttributes;
    featureExistsInSuper = args.featureExistsInSuper;
    featureExistsInExtension = args.featureExistsInExtension;

    userIntent = args.userIntent;
    onNotExists = args.onNotExists;
    onExists = args.onExists;
    onVersionConflict = args.onVersionConflict;
    onMergeConflict = args.onMergeConflict;
    spaceContext = args.spaceContext;

    assertions = args.assertions;

    if (spaceContext == null)
      spaceContext = EXTENSION; //TODO: Check if that default makes sense

    if (composite && featureExists && !featureExistsInSuper && !featureExistsInExtension)
      throw new IllegalArgumentException("Illegal test arguments: Given a composite space, the existing feature "
          + "must exist at least in one of the composing spaces.");

    if (!composite && featureExistsInSuper)
      throw new IllegalArgumentException("Illegal test arguments: Given a non-composite space, the existing feature "
          + "can not exist in a super space.");

    if (!composite && featureExistsInExtension)
      throw new IllegalArgumentException("Illegal test arguments: Given a non-composite space, the existing feature "
          + "can not exist in an extension space.");
  }

  private static Feature featureWithEmptyProperties() {
    return new Feature()
        .withId(TEST_FEATURE_ID)
        .withGeometry(TEST_FEATURE_GEOMETRY)
        .withProperties(new Properties());
  }

  protected static Feature simpleFeature() throws JsonProcessingException {
    return featureWithEmptyProperties().withProperties(new Properties().with("firstName", "Alice").with("age", 35));
  }

  protected static Feature modifiedFeature(long baseVersion) throws JsonProcessingException {
    Feature feature = simpleFeature();
    feature.getProperties()
        .with("modifiedField", "someValue")
        .with("otherModifiedField", 27);

    if (baseVersion > -1)
      feature.getProperties().withXyzNamespace(new XyzNamespace().withVersion(baseVersion));

    return feature;
  }

  protected static Feature concurrentlyModifiedFeature(boolean withConflictingAttributes) throws JsonProcessingException {
    Feature feature = simpleFeature();
    feature.getProperties()
        .with("modifiedField", withConflictingAttributes ? "someConflictingValue" : "someValue")
        .with("someOtherConcurrentField", "someOtherValue");
    return feature;
  }

  protected static Feature deletedFeature(long version) {
    Feature feature = featureWithEmptyProperties();
    feature.getProperties().withXyzNamespace(new XyzNamespace()
        .withDeleted(true)
        .withVersion(version));
    return feature;
  }

  protected static Feature mergedFeature(long version) throws JsonProcessingException {
    Feature feature = modifiedFeature(version);
    feature.getProperties().with("someOtherConcurrentField", "someOtherValue");
    return feature;
  }

  @BeforeEach
  public void prepare() throws Exception {
    init(modifyArgs(EMPTY_ARGS));
    spaceWriter().createSpaceResources();
  }

  @AfterEach
  public void clean() throws Exception {
    spaceWriter().cleanSpaceResources();
  }

  private void writeFeatureForPreparation(Feature feature, String author, SpaceContext context) throws Exception {
    new SQLSpaceWriter(composite, getClass().getSimpleName()).writeFeature(feature, author, null, null, null, null,
        false, context, history);
    writtenSpaceVersions.get(context).increment();
  }

  public void runTest(TestArgs args) throws Exception {
    init(modifyArgs(args));
    runTest();
  }

  public void runTest() throws Exception {
    //------------ Prepare preconditions ------------

    /*
    NOTE regarding the base version to be used in the actual test (`writtenSpaceVersions` field):
    Depending on the amount of writes before the actual test-write,
    the precondition preparation will increase the base version to the correct value to be used later during the test.

    E.g.: The first change in the space is version 1 - its base version is 0
    If one (non-conflicting) feature version was written in the preparation phase, the base version will be: 1

    In case the base version should not be matched (version-conflict case), two feature versions would be written.
    First the base feature version (the actual test will base on), then the 2nd conflicting feature version.
    In that case, the base version used in the actual test must still be 1, because otherwise
    the "concurrently" written feature version would not cause a conflict.
     */
    if (featureExists) {
      //The test expects a pre-existing feature with the same ID, so create one upfront
      writeFeatureForPreparation(simpleFeature(), DEFAULT_AUTHOR, featureExistsInSuper ? SUPER : EXTENSION);
      baseVersion++;

      if (featureExistsInSuper && featureExistsInExtension)
        //The test expects a 2nd pre-existing feature with the same ID in the extension space, so create it as well
        writeFeatureForPreparation(simpleFeature(), DEFAULT_AUTHOR, EXTENSION);

      if (!baseVersionMatch) {
        /*
        The test expects the pre-existing feature's version
        to be not equal with the base version of the feature that later is being used in the actual test.
        (E.g., Some other user wrote some new version in between)
        So create a 2nd (conflicting) version of the feature (as if it had been written by another user)
        NOTE: For this write the written space versions are not increased,
          because the conflicting write should lead to a base version mismatch.

        Depending on `conflictingAttributes`, the test expects
        that at least the value of one attribute is conflicting with one attribute-value of the actual test-feature.
         */
        SpaceContext context = featureExistsInSuper && featureExistsInExtension ? EXTENSION : featureExistsInSuper ? SUPER : EXTENSION;
        writeFeatureForPreparation(concurrentlyModifiedFeature(conflictingAttributes), OTHER_AUTHOR, context);
      }
    }

    //Create a snapshot of the space state *before* the test execution
    beforeState = gatherSpaceState();
    long beforeTs = System.currentTimeMillis();

    //------------ Perform the actual test call ------------
    try {
      //TODO: Also support "partial" to be influenced through test args
      spaceWriter().writeFeature(modifiedFeature(baseVersion), UPDATE_AUTHOR, onExists, onNotExists,
          onVersionConflict, onMergeConflict,false, spaceContext, history);
    }
    catch (SQLException e) {
      thrownError = SQLError.fromErrorCode(e.getSQLState());
      //Rethrow the exception if it was not one of the expected ones
      if (thrownError == UNKNOWN)
        throw e;
    }

    //Create a snapshot of the space state *after* the test execution
    afterState = gatherSpaceState();

    //------------ Check the assertions against the results ------------
    checkAssertions(beforeTs);
  }

  private void checkAssertions(long beforeTestStartTimestamp) throws Exception {
    //Check the thrown errors (only against expected ones)
    assertEquals((thrownError == null ? "No " : assertions.sqlError != null ? "Wrong " : "An ") + "error was thrown" + (assertions.sqlError == null ? " but none was expected." : thrownError == null ? " but it was expected one." : ""), assertions.sqlError, thrownError);

    //Check the table operation
    SpaceTableState beforeTableState = beforeState.tableStateForContext(spaceContext);
    SpaceTableState afterTableState = afterState.tableStateForContext(spaceContext);
    TableOperation performedTableOperation = inferTableOperation(beforeTableState, afterTableState);
    assertEquals("A wrong table operation was performed.", assertions.performedTableOperation, performedTableOperation);

    //Check whether the feature was written properly
    //TODO: Expect version increase on table.DELETE?
    if (performedTableOperation == INSERT || performedTableOperation == UPDATE) {
      //Check the used feature operation
      Operation featureOperation = afterTableState.lastUsedFeatureOperation;
      assertEquals("A wrong feature operation was used.", assertions.usedFeatureOperation, featureOperation);

      //Check the written feature content
      long expectedVersion = getWrittenSpaceVersion(spaceContext) + 1;
      Feature expectedFeature = Set.of(D, H, J).contains(featureOperation) ? deletedFeature(expectedVersion)
          : assertions.featureWasMerged ? mergedFeature(expectedVersion) : modifiedFeature(expectedVersion);
      expectedFeature.getProperties().getXyzNamespace()
          .withCreatedAt(afterTableState.feature.getProperties().getXyzNamespace().getCreatedAt())
          .withUpdatedAt(afterTableState.feature.getProperties().getXyzNamespace().getUpdatedAt())
          .withAuthor(UPDATE_AUTHOR);
      applyAuthorWorkaround(expectedFeature);
      assertEquals("The feature was written incorrectly.", expectedFeature, afterTableState.feature);

      //Check if the feature's update timestamp has been written properly
      Matcher<Long> isGreaterThanBeforeTimestamp = greaterThan(beforeTestStartTimestamp);
      assertThat("The feature's update timestamp has to be higher than the timestamp when the test started.",
          afterTableState.feature.getProperties().getXyzNamespace().getUpdatedAt(), isGreaterThanBeforeTimestamp);
      //Check if the feature's creation timestamp has been written properly
      assertThat("The feature's creation timestamp has to be " + (featureOperation == I || featureOperation == H ? "higher" : "lower")
          + " than the timestamp when the test started.", afterTableState.feature.getProperties().getXyzNamespace().getCreatedAt(),
          featureOperation == I || featureOperation == H ? isGreaterThanBeforeTimestamp : not(isGreaterThanBeforeTimestamp));
    }
    else if (performedTableOperation == NONE) {
      //Check the state of the feature operation
      assertEquals("A feature operation was applied even if the expected table operation was NONE.",
          beforeTableState.lastUsedFeatureOperation, afterTableState.lastUsedFeatureOperation);

      //Check the feature content (including timestamps)
      assertEquals("The feature content has changed even if the expected table operation was NONE.",
          beforeTableState.feature, afterTableState.feature);
    }
    else if (performedTableOperation == DELETE) {
      //Check whether the feature was actually deleted
      assertEquals(null, afterTableState.feature);
    }
  }

  private void applyAuthorWorkaround(Feature expectedFeature) {
    if (applyAuthorWorkaround)
      expectedFeature.getProperties().getXyzNamespace().withAuthor("ANONYMOUS");
  }

  private long getWrittenSpaceVersion(SpaceContext context) {
    return writtenSpaceVersions.get(context == DEFAULT ? EXTENSION : context).longValue();
  }

  private TableOperation inferTableOperation(SpaceTableState before, SpaceTableState after) {
    if (after.featureVersionCount < before.featureVersionCount)
      return DELETE;
    if (after.featureVersionCount > before.featureVersionCount)
      return INSERT;
    if (!featureEquals(after.feature, before.feature))
      return UPDATE;
    return NONE;
  }

  private SpaceState gatherSpaceState() throws Exception {
    if (composite)
      return new SpaceState(gatherSpaceTableState(EXTENSION), gatherSpaceTableState(SUPER));
    else
      return new SpaceState(gatherSpaceTableState(DEFAULT));
  }

  private SpaceTableState gatherSpaceTableState(SpaceContext context) throws Exception {
    Feature feature = spaceWriter().getFeature(context);
    Operation lastUsedFeatureOperation = spaceWriter().getLastUsedFeatureOperation(context);
    return history
        ? new SpaceTableState(feature, lastUsedFeatureOperation, spaceWriter().getRowCount(context))
        : new SpaceTableState(feature, lastUsedFeatureOperation);
  }

  public enum TableOperation {
    INSERT,
    UPDATE,
    DELETE,
    NONE
  }

  protected enum UserIntent {
    WRITE, //Illegal Argument
    DELETE
  }

  private boolean featureEquals(Feature feature1, Feature feature2) {
    if (feature1 == null || feature2 == null)
      return Objects.equals(feature1, feature2);
    //TODO: Proper implementation
    return feature1.serialize().equals(feature2.serialize());
  }

  //TODO: Use unboxed type instead of Booleans
  public record TestArgs(String testName, boolean composite, boolean history, boolean featureExists, Boolean baseVersionMatch,
      Boolean conflictingAttributes, Boolean featureExistsInSuper, Boolean featureExistsInExtension, UserIntent userIntent,
      OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
      SpaceContext spaceContext, TestAssertions assertions) {

    public TestArgs {
      if (featureExistsInSuper == null)
        featureExistsInSuper = false;

      if (featureExistsInExtension == null)
        featureExistsInExtension = false;
    }

    public TestArgs(String testName, boolean composite, boolean history, boolean featureExists, boolean baseVersionMatch,
        Boolean featureExistsInSuper, Boolean featureExistsInExtension, UserIntent userIntent,
        OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
        SpaceContext spaceContext, TestAssertions assertions) {
      this(testName, composite, history, featureExists, baseVersionMatch, false, featureExistsInSuper,
          featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, assertions);
    }

    public TestArgs(String testName, boolean composite, boolean history, Boolean baseVersionMatch,
        Boolean conflictingAttributes, UserIntent userIntent,
        OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, TestAssertions assertions) {
      this(testName, composite, history, false, baseVersionMatch, conflictingAttributes, false,
          false, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, DEFAULT, assertions);
    }

    public TestArgs(String testName, boolean composite, boolean history, UserIntent userIntent,
        OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, TestAssertions assertions) {
      this(testName, composite, history, true, false, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, assertions);
    }

    public TestArgs(String testName, boolean history, boolean featureExists, Boolean baseVersionMatch,
        Boolean conflictingAttributes, UserIntent userIntent, OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict,
        OnMergeConflict onMergeConflict, SpaceContext spaceContext, TestAssertions assertions) {
      this(testName, false, history, featureExists, baseVersionMatch, conflictingAttributes, false,
          false, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, assertions);
    }

    public TestArgs(String testName, boolean history, boolean featureExists, Boolean baseVersionMatch, UserIntent userIntent,
        OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
        SpaceContext spaceContext, TestAssertions assertions) {
      this(testName, false, history, featureExists, baseVersionMatch, false, false,
          false, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext, assertions);
    }

    public TestArgs(String testName, boolean history, boolean featureExists, UserIntent userIntent,
        OnNotExists onNotExists, OnExists onExists, OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict,
        SpaceContext spaceContext, TestAssertions assertions) {
      this(testName, false, history, featureExists, true, false, false,
          false, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext,
          assertions);
    }

    public TestArgs withComposite(boolean composite) {
      return new TestArgs(testName, true, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper,
          featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext,
          assertions);
    }

    public TestArgs withContext(SpaceContext spaceContext) {
      return new TestArgs(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper,
          featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext,
          assertions);
    }

    public TestArgs withFeatureExistsInSuper(boolean featureExistsInSuper) {
      return new TestArgs(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper,
          featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext,
          assertions);
    }

    public TestArgs withFeatureExistsInExtension(boolean featureExistsInExtension) {
      return new TestArgs(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper,
          featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext,
          assertions);
    }

    public TestArgs withHistory(boolean history) {
      return new TestArgs(testName, composite, history, featureExists, baseVersionMatch, conflictingAttributes, featureExistsInSuper,
          featureExistsInExtension, userIntent, onNotExists, onExists, onVersionConflict, onMergeConflict, spaceContext,
          assertions);
    }

    @Override
    public String toString() {
      return testName + ": " + humanReadableName();
    }

    private String humanReadableName() {
      String name = userIntent == UserIntent.DELETE ? "DeleteFeature from " : "WriteFeature to ";

      name += composite ? "Composite" : "";
      name += "Space ";
      if (history)
        name += "with history ";

      String featureConditions = humanReadableFeatureConditions();
      if (!featureConditions.trim().isEmpty())
        name += " and " + featureConditions;
      name += " -> ";

      List<String> desiredActions = new ArrayList<>();
      if (onNotExists != null)
        desiredActions.add(toHumanReadable(onNotExists) + "IfNotExists");

      if (onExists != null)
        desiredActions.add(toHumanReadable(onExists) + "IfExists");

      if (onVersionConflict != null && !baseVersionMatch) {
        desiredActions.add(toHumanReadable(onVersionConflict) + "OnVersionConflict");
        if (conflictingAttributes && onVersionConflict == OnVersionConflict.MERGE && conflictingAttributes && onMergeConflict != null)
          desiredActions.add(toHumanReadable(onMergeConflict) + "OnMergeConflict");
      }

      name += String.join(", ", desiredActions);

      return name;
    }

    private String humanReadableFeatureConditions() {
      List<String> featureConditions = new ArrayList<>();


      String exists = "feature" + (featureExists ? "" : "Not") + "Exists";
      if (featureExists && composite)
        exists += "In(" + (featureExistsInSuper && featureExistsInExtension ? SUPER + " & " + EXTENSION : featureExistsInSuper ? SUPER : EXTENSION) + ")";
      featureConditions.add(exists);

      if (!baseVersionMatch) {
        String conflict = "conflictingChange";
        if (onVersionConflict == OnVersionConflict.MERGE && conflictingAttributes)
          conflict += "WithAttributesConflict";
        featureConditions.add(conflict);
      }

      return String.join(" and ", featureConditions);
    }

    private String toHumanReadable(OnNotExists onNotExists) {
      return switch (onNotExists) {
        case CREATE -> "Create";
        case RETAIN -> "DoNothing";
        case ERROR -> "ThrowError";
      };
    }

    private String toHumanReadable(OnExists onExists) {
      return switch (onExists) {
        case DELETE -> "Delete";
        case REPLACE -> "Replace";
        case RETAIN -> "Keep";
        case ERROR -> "ThrowError";
      };
    }

    private String toHumanReadable(OnVersionConflict onVersionConflict) {
      return switch (onVersionConflict) {
        case MERGE -> "Merge";
        case REPLACE -> "Replace";
        case RETAIN -> "DoNothing";
        case ERROR -> "ThrowError";
        case DELETE -> "Delete";
      };
    }

    private String toHumanReadable(OnMergeConflict onMergeConflict) {
      return switch (onMergeConflict) {
        case REPLACE -> "Replace";
        case RETAIN -> "KeepCurrent";
        case ERROR -> "ThrowError";
      };
    }
  }

  public record SpaceTableState(Feature feature, Operation lastUsedFeatureOperation, int featureVersionCount) {

    /**
     * Use this constructor if the space has no history
     * @param feature
     */
    public SpaceTableState(Feature feature, Operation lastUsedFeatureOperation) {
      this(feature, lastUsedFeatureOperation,  feature == null ? 0 : 1);
    }
  }

  public record SpaceState(SpaceTableState spaceTableState, SpaceTableState superSpaceTableState) {

    /**
     * Use this constructor if the space is no composite space
     * @param spaceTableState
     */
    public SpaceState(SpaceTableState spaceTableState) {
      this(spaceTableState, null);
    }

    public SpaceTableState tableStateForContext(SpaceContext context) {
      return context == SUPER ? superSpaceTableState : spaceTableState;
    }
  }

  public record TestAssertions(TableOperation performedTableOperation, Operation usedFeatureOperation, boolean featureWasMerged,
      SQLError sqlError) {

    public TestAssertions {
      if (performedTableOperation == null)
        throw new NullPointerException("The asserted table operation can not be null. Use NONE if no action on the table is asserted!");
      if (usedFeatureOperation == null && performedTableOperation != DELETE && performedTableOperation != NONE)
        throw new IllegalArgumentException("If no feature operation is asserted, the only asserted table operations can be DELETE or NONE.");
      if (usedFeatureOperation != null && (performedTableOperation == NONE || performedTableOperation == DELETE))
        throw new IllegalArgumentException("If the asserted table operation is NONE or DELETE, no feature operation can be asserted.");
    }

    public TestAssertions(TableOperation tableOperation, Operation featureOperation, boolean featureWasMerged) {
      this(tableOperation, featureOperation, featureWasMerged, null);
    }

    public TestAssertions(TableOperation tableOperation, Operation featureOperation) {
      this(tableOperation, featureOperation, false);
    }

    public TestAssertions(TableOperation tableOperation) {
      this(tableOperation, null);
    }

    public TestAssertions(SQLError sqlError) {
      this(NONE, null, false, sqlError);
    }

    public TestAssertions() {
      this(NONE);
    }
  }
}
