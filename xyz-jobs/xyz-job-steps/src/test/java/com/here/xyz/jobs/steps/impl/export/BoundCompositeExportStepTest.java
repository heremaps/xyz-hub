/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.impl.export;

import static com.here.xyz.models.hub.Ref.HEAD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.outputs.DownloadUrl;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exports of a composite space which is bound to a fixed version of its base space via {@code extends.version}.
 *
 * NOTE: Unlike {@link CompositeExportStepTest}, these cases assert explicit expected values instead of diffing the export against
 * the Hub's own read of the same space and context. That comparison would also pass if the export *and* the Hub both ignored the
 * bound version, which is exactly the regression these tests exist to catch.
 */
public class BoundCompositeExportStepTest extends ExportTestBase {

  private static final String BOUND_FEATURE_ID = "bound_point";
  private static final long BOUND_BASE_VERSION = 1;

  /**
   * The state of the base space at {@link #BOUND_BASE_VERSION}, and the state it moved on to afterwards.
   */
  private static final String STATE_AT_BOUND_VERSION = "initial";
  private static final String STATE_AT_BASE_HEAD = "moved";

  private final String BASE_SPACE_ID = SPACE_ID + "_bound_base";
  private final String COMPOSITE_SPACE_ID = SPACE_ID + "_bound_ext";

  /**
   * Leaves the base space holding two versions of {@link #BOUND_FEATURE_ID} and a composite bound to the first of them, so that the
   * bound snapshot and the base's HEAD are distinguishable.
   */
  @BeforeEach
  public void setUp() throws Exception {
    createSpace(new Space().withId(BASE_SPACE_ID).withVersionsToKeep(100), false);
    putFeatureCollectionToSpace(BASE_SPACE_ID, boundFeature(STATE_AT_BOUND_VERSION)); //base version 1

    createSpace(new Space().withId(COMPOSITE_SPACE_ID).withVersionsToKeep(100)
        .withExtension(new Space.Extension().withSpaceId(BASE_SPACE_ID).withVersion(BOUND_BASE_VERSION)), false);

    //The base moves on after the composite was bound
    putFeatureCollectionToSpace(BASE_SPACE_ID, boundFeature(STATE_AT_BASE_HEAD)); //base version 2
  }

  @AfterEach
  public void cleanup() throws SQLException {
    super.cleanup();
    //The extension has to go first, a base space can not be deleted while it is being extended
    deleteSpace(COMPOSITE_SPACE_ID);
    deleteSpace(BASE_SPACE_ID);
  }

  @Test
  public void exportOfABoundCompositeReadsTheBoundBaseVersion() throws Exception {
    assertExportedState(COMPOSITE_SPACE_ID, SpaceContext.DEFAULT, STATE_AT_BOUND_VERSION);
  }

  /**
   * The binding belongs to the composite, so exporting the base space on its own is unaffected by it.
   */
  @Test
  public void exportOfTheBaseOfABoundCompositeStillReadsItsHead() throws Exception {
    assertExportedState(BASE_SPACE_ID, SpaceContext.DEFAULT, STATE_AT_BASE_HEAD);
  }

  /**
   * A SUPER export of a bound composite is refused outright: the ref reaching the step carries no indication of which space it was
   * resolved against, or whether it was resolved at all, so there is no sound way to interpret it.
   */
  @Test
  public void exportOfABoundCompositeWithContextSuperIsRejected() {
    assertSuperExportIsRejected(new Ref(HEAD));
  }

  /**
   * The rejection is unconditional on the requested version - a ref which happens to name the bound version is refused too. Allowing
   * that case would be unsound, because by the time the step sees the ref it can not tell whether the caller asked for that version
   * or whether it is the result of a resolution.
   */
  @Test
  public void exportOfABoundCompositeWithContextSuperIsRejectedEvenForTheBoundVersion() {
    assertSuperExportIsRejected(new Ref(BOUND_BASE_VERSION));
  }

  private void assertSuperExportIsRejected(Ref versionRef) {
    ExportSpaceToFiles step = exportStep(COMPOSITE_SPACE_ID, SpaceContext.SUPER, versionRef);

    RuntimeException thrown = assertThrows(RuntimeException.class, () -> sendLambdaStepRequestBlock(step, true));
    //Assert on the reason, so that an unrelated failure (e.g. an unreachable Hub) does not satisfy this test
    assertTrue(thrown.getMessage().contains("not supported for a resource which extends a specific version"),
        "A SUPER export of a bound composite has to be rejected, but failed with: " + thrown.getMessage());
  }

  private static FeatureCollection boundFeature(String state) {
    return new FeatureCollection().withFeatures(List.of(new Feature()
        .withId(BOUND_FEATURE_ID)
        .withProperties(new Properties().with("value", state))
        .withGeometry(new Point().withCoordinates(new PointCoordinates(8.47, 50.05)))));
  }

  private ExportSpaceToFiles exportStep(String spaceId, SpaceContext context, Ref versionRef) {
    ExportSpaceToFiles step = new ExportSpaceToFiles()
        .withThreadCount(1)
        .withSpaceId(spaceId)
        .withJobId(JOB_ID);
    step.setContext(context);
    step.setVersionRef(versionRef);
    return step;
  }

  private void assertExportedState(String spaceId, SpaceContext context, String expectedState) throws Exception {
    //HEAD is the case a bound base version overrides rather than rejects
    ExportSpaceToFiles step = exportStep(spaceId, context, new Ref(HEAD));
    sendLambdaStepRequestBlock(step, true);

    List<Feature> exportedFeatures = new ArrayList<>();
    for (Output output : step.loadOutputs())
      if (output instanceof DownloadUrl downloadUrl)
        exportedFeatures.addAll(downloadFileAndDeserializeFeatures(downloadUrl));

    Feature exported = exportedFeatures.stream()
        .filter(feature -> BOUND_FEATURE_ID.equals(feature.getId()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("The feature \"" + BOUND_FEATURE_ID + "\" was not exported from " + spaceId
            + " with context " + context));

    assertEquals(expectedState, exported.getProperties().get("value"),
        "Exported the wrong base state from " + spaceId + " with context " + context);
  }
}
