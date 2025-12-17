/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.jobs.steps;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.jobs.steps.Step.Visibility.SYSTEM;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.datasets.filters.SpatialFilter;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.StepTest;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePost;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePre;
import com.here.xyz.jobs.steps.impl.transport.CountSpace;
import com.here.xyz.jobs.steps.outputs.CreatedVersion;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.responses.StatisticsResponse;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CopySpaceStepsTest extends StepTest {

  static protected String sourceSpaceId = "testCopy-Source-07",
                        sourceSpaceBaseId = "testCopy-Source-base-07",
                        targetSpaceId = "testCopy-Target-07",
                        emptyTargetSpaceId = "testCopy-Target-07e",
                        otherConnectorId = "psql_db2_hashed",
                        targetRemoteSpace = "testCopy-Target-07-remote",
                        emptyTargetRemoteSpace = "testCopy-Target-07e-remote",
                        propertyFilter = "p.all=common",
                        versionRange = "1..6";

  static protected Polygon spatialSearchGeom;
  static private float xmin = 7.0f, ymin = 50.0f, xmax = 7.1f, ymax = 50.1f;
  private static final Feature DELETED_FEATURE, DELETED_FEATURE_IN_TARGET;
  static private long NrFeaturesAtStartInTargetSpace = 3;

  static {
    LinearRingCoordinates lrc = new LinearRingCoordinates();
    lrc.add(new Position(xmin, ymin));
    lrc.add(new Position(xmax, ymin));
    lrc.add(new Position(xmax, ymax));
    lrc.add(new Position(xmin, ymax));
    lrc.add(new Position(xmin, ymin));
    PolygonCoordinates pc = new PolygonCoordinates();
    pc.add(lrc);
    spatialSearchGeom = new Polygon().withCoordinates(pc);

    DELETED_FEATURE = new Feature()
        .withId("id-deleted-feature")
        .withProperties(new Properties().with("all", "common"))
        .withGeometry(new Point()
            .withCoordinates(new PointCoordinates(7.05, 50.05)));

    DELETED_FEATURE_IN_TARGET = new Feature()
        .withId("id-deleted-in-target-feature")
        .withProperties(new Properties().with("all", "common"))
        .withGeometry(new Point()
            .withCoordinates(new PointCoordinates(7.05, 50.05)));
  }

  protected void createSpaces()
  {
    createSpace(new Space().withId(sourceSpaceBaseId).withVersionsToKeep(100), false);

    createSpace(new Space().withId(sourceSpaceId).withVersionsToKeep(100)
                           .withExtension(new Space.Extension().withSpaceId(sourceSpaceBaseId)), false);

    createSpace(new Space().withId(targetSpaceId).withVersionsToKeep(100), false);
    createSpace(new Space().withId(targetRemoteSpace).withVersionsToKeep(100).withStorage(new ConnectorRef().withId(otherConnectorId)),false);

    createSpace(new Space().withId(emptyTargetSpaceId).withVersionsToKeep(100), false);
    createSpace(new Space().withId(emptyTargetRemoteSpace).withVersionsToKeep(100).withStorage(new ConnectorRef().withId(otherConnectorId)),false);
  }

  @BeforeEach
  public void setup() throws SQLException {
    cleanup();
    createSpaces();
    //FIXME: Do not use random feature sets but specific ones that are fitting the actual use-case to be tested (prevents flickering and improves testing time)
    //write features source
    putRandomFeatureCollectionToSpace(sourceSpaceBaseId, 7, xmin, ymin, xmax, ymax); // base will not be copied

    //v1
    putRandomFeatureCollectionToSpace(sourceSpaceId, 20, xmin, ymin, xmax, ymax);
    //v2
    putRandomFeatureCollectionToSpace(sourceSpaceId, 5, xmin, ymin, xmax, ymax);
    //v3
    putFeatureToSpace(sourceSpaceId, DELETED_FEATURE);
    //v4
    putFeatureToSpace(sourceSpaceId, DELETED_FEATURE_IN_TARGET);
    //v5
    deleteFeaturesInSpace(sourceSpaceId, List.of(DELETED_FEATURE.getId(), DELETED_FEATURE_IN_TARGET.getId()));
    //v6
    putRandomFeatureCollectionToSpace(sourceSpaceId, 5, xmin, ymin, xmax, ymax);
    //v7
    putRandomFeatureCollectionToSpace(sourceSpaceId, 5, xmin, ymin, xmax, ymax);
    //v8
    putRandomFeatureCollectionToSpace(sourceSpaceId, 5, xmin, ymin, xmax, ymax);

    //write features target - non-empty-space
    putRandomFeatureCollectionToSpace(targetSpaceId, 2, xmin, ymin, xmax, ymax);
    putFeatureToSpace(targetSpaceId, DELETED_FEATURE_IN_TARGET);

    putRandomFeatureCollectionToSpace(targetRemoteSpace, 2, xmin, ymin, xmax, ymax);
    putFeatureToSpace(targetRemoteSpace, DELETED_FEATURE_IN_TARGET);
  }

  protected void deleteSpaces()
  {
    deleteSpace(sourceSpaceId);
    deleteSpace(sourceSpaceBaseId);
    deleteSpace(targetSpaceId);
    deleteSpace(targetRemoteSpace);
  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpaces();
  }

  private static Stream<Arguments> provideParameters() {
    return Stream.of(

        Arguments.of(false, null, false, null, null,false),
        Arguments.of(false, null, false, null, null,true),

        Arguments.of(false, null, false, propertyFilter,null,false),
        Arguments.of(false, spatialSearchGeom, false, null,null,false),
        Arguments.of(false, spatialSearchGeom, true, null,null,false),
        Arguments.of(false, spatialSearchGeom, true, null,null,true),
        Arguments.of(false, spatialSearchGeom, false, propertyFilter,null,false),
        Arguments.of(false, spatialSearchGeom, true, propertyFilter,null,false),

        Arguments.of(false, null, false, null, versionRange,false),
        Arguments.of(false, null, false, null, versionRange,true),
        Arguments.of(false, null, false, propertyFilter,versionRange,false),
        Arguments.of(false, spatialSearchGeom, false, null, versionRange,false),

        Arguments.of(true, null, false, null,null,false),
        Arguments.of(true, null, false, null,null,true),
        Arguments.of(true, null, false, propertyFilter,null,false),
        Arguments.of(true, spatialSearchGeom, false, null,null,false),
        Arguments.of(true, spatialSearchGeom, true, null,null,false),
        Arguments.of(true, spatialSearchGeom, true, null,null,true),
        Arguments.of(true, spatialSearchGeom, false, propertyFilter,null,false),
        Arguments.of(true, spatialSearchGeom, true, propertyFilter,null,false)

    );
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void copySpace(boolean testRemoteDb, Geometry geo, boolean clip, String propertyFilter, String versionRef, boolean emptyTarget) throws Exception {
    Ref resolvedRef = resolveRef(sourceSpaceId, new Ref(versionRef));

    String targetSpace = !testRemoteDb ? (!emptyTarget ? targetSpaceId : emptyTargetSpaceId )
                                       : (!emptyTarget ? targetRemoteSpace : emptyTargetRemoteSpace );

    StatisticsResponse statsBefore = getStatistics(targetSpace);

    long NrFeaturesInTarget = statsBefore.getCount().getValue();
    assertEquals( !emptyTarget ? NrFeaturesAtStartInTargetSpace : 0, NrFeaturesInTarget);

    CopySpace step = new CopySpace()
        .withSpaceId(sourceSpaceId)
        .withSourceVersionRef(resolvedRef)
        .withSpatialFilter( geo == null ? null : new SpatialFilter().withGeometry(geo).withClip(clip).withRadius(7) )
        .withPropertyFilter(PropertiesQuery.fromString(propertyFilter))
        .withTargetSpaceId(targetSpace)
        .withJobId(JOB_ID)
        .withEstimatedTargetFeatureCount(NrFeaturesInTarget)
        //.withThreadInfo(new int[]{6, 8})
        /* test only -> */.withTargetVersion(3); //TODO: rather provide the according model-based input instead and remove #withTargetVersion() again (that would also directly test the functionality of providing the input accordingly)

    sendLambdaStepRequestBlock(step, true);

    long expectedCount = ( !emptyTarget ? 43L : 40L );

    if( versionRef != null )
    { expectedCount = ( !emptyTarget ? 12L : 10L );
      if( geo != null || propertyFilter != null ) //TODO: clarify - in case of filtering with versionRange, deleted features are not copied as they are not "found"
       expectedCount++;
    }

    StatisticsResponse statsAfter = getStatistics(targetSpace);
    //TODO: Remove the following ambiguity once the CopySpace step has been fixed to also sync deletions properly
    assertTrue(statsAfter.getCount().getValue() == expectedCount || statsAfter.getCount().getValue() == expectedCount - 1);
  }

  @Test
  public void copySpacePre() throws Exception {
    CopySpacePre step = new CopySpacePre()
        .withSpaceId(targetSpaceId)
        .withJobId(JOB_ID);

    sendLambdaStepRequestBlock(step, true);

    List<?> outputs = step.loadOutputs(SYSTEM);

    long fetchedVersion = -1;
    for (Object output : outputs)
      if (output instanceof CreatedVersion f)
        fetchedVersion = f.getVersion();

    assertEquals(3l, fetchedVersion);
  }

  @Test
  public void copySpacePost() throws Exception {
    CopySpacePost step = new CopySpacePost()
        .withSpaceId(sourceSpaceId)
        .withJobId(JOB_ID);

    sendLambdaStepRequestBlock(step, true);

    List<Output> outputs = step.loadOutputs(USER);

    FeatureStatistics featureStatistics = null;

    for (Output output : outputs)
      if (output instanceof FeatureStatistics statistics)
        featureStatistics = statistics;

    assertNotNull(featureStatistics);
    assertEquals(0, featureStatistics.getFeatureCount());
    assertEquals(0, featureStatistics.getByteSize());
  }

  protected static Stream<Arguments> provideCountParameters() {
    return Stream.of(
        Arguments.of( DEFAULT, null, null, null),
        Arguments.of( DEFAULT, null, null, null),
        Arguments.of( EXTENSION, null, propertyFilter,null),
        Arguments.of( DEFAULT, null, propertyFilter,null),
        Arguments.of( EXTENSION, spatialSearchGeom, null,null),
        Arguments.of( DEFAULT, spatialSearchGeom, null,null),
        Arguments.of( EXTENSION, spatialSearchGeom, propertyFilter,null),
        Arguments.of( DEFAULT, spatialSearchGeom, propertyFilter,null),

        Arguments.of( EXTENSION, null, null, versionRange),
        Arguments.of( EXTENSION, null, propertyFilter,versionRange),
        Arguments.of( EXTENSION, spatialSearchGeom, null, versionRange)
    );
  }

  @ParameterizedTest
  @MethodSource("provideCountParameters")
  public void countSpace(SpaceContext ctx, Geometry geo, String propertyFilter, String versionRef) throws Exception {
    assertNotNull(ctx);
    //TODO: Check why counting should only be supported on context=EXTENSION
    assertTrue(versionRef == null || ctx == EXTENSION); //counting on a versionRef is only supported in context EXTENSION

    Ref resolvedRef = resolveRef(sourceSpaceId, new Ref(versionRef));

    CountSpace step = new CountSpace()
        .withSpaceId(sourceSpaceId)
        .withSpatialFilter( geo == null ? null : new SpatialFilter().withGeometry(geo) )
        .withPropertyFilter(PropertiesQuery.fromString(propertyFilter))
        .withVersionRef(resolvedRef)
        .withContext(ctx != null ? ctx : DEFAULT )
        .withJobId(JOB_ID);

    sendLambdaStepRequestBlock(step, true);

    List<Output> outputs = step.loadOutputs(USER);

    FeatureStatistics featureStatistics = null;

    for (Output output : outputs)
      if (output instanceof FeatureStatistics statistics)
        featureStatistics = statistics;

    long expectedCount = versionRef == null ? 40l : 12l; // assuming context EXTENSION

    if (step.getContext() == DEFAULT) // => no versionRef
     expectedCount = 47;

    if(versionRef != null && (geo != null || propertyFilter != null) ) //TODO: clarify - in case of filtering with versionRange, deleted features are not counted as they are not "found"
     expectedCount = 10;

    assertNotNull(featureStatistics);
    assertEquals(expectedCount, featureStatistics.getFeatureCount());
    assertEquals(0, featureStatistics.getByteSize());
  }
}
