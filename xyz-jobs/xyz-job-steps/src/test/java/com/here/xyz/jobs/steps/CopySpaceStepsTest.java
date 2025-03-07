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

package com.here.xyz.jobs.steps;

import static com.here.xyz.jobs.steps.Step.Visibility.SYSTEM;
import static com.here.xyz.jobs.steps.Step.Visibility.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
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
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Polygon;
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

  static private String sourceSpaceId = "testCopy-Source-07",
                        sourceSpaceBaseId = "testCopy-Source-base-07",
                        targetSpaceId = "testCopy-Target-07",
                        otherConnectorId = "psql_db2_hashed",
                        targetRemoteSpace = "testCopy-Target-07-remote",
                        propertyFilter = "p.all=common",
                        versionRange = "1..6";

  static private Polygon spatialSearchGeom;
  static private float xmin = 7.0f, ymin = 50.0f, xmax = 7.1f, ymax = 50.1f;
  static private FeatureCollection ftCollection, ftCollection2;
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

    try {
     ftCollection = XyzSerializable.deserialize(
      """
       {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": "id-deleted-feature",
                "properties": { "all":"common" },
                "geometry": {"type": "Point", "coordinates": [7.05,50.05]}
            }
        ]
       }
      """, FeatureCollection.class);
      ftCollection2 = XyzSerializable.deserialize(
        """
         {
          "type": "FeatureCollection",
          "features": [
              {
                  "type": "Feature",
                  "id": "id-deleted-in-target-feature",
                  "properties": { "all":"common" },
                  "geometry": {"type": "Point", "coordinates": [7.05,50.05]}
              }
          ]
         }
        """, FeatureCollection.class);
    } catch (JsonProcessingException e) {
     e.printStackTrace();
    }

  }


  @BeforeEach
  public void setup() throws SQLException {
    cleanup();
    createSpace(new Space().withId(sourceSpaceBaseId).withVersionsToKeep(100), false);

    createSpace(new Space().withId(sourceSpaceId).withVersionsToKeep(100)
                           .withExtension(new Space.Extension().withSpaceId(sourceSpaceBaseId)), false);

    createSpace(new Space().withId(targetSpaceId).withVersionsToKeep(100), false);
    createSpace(new Space().withId(targetRemoteSpace).withVersionsToKeep(100).withStorage(new ConnectorRef().withId(otherConnectorId)),
        false);

    //FIXME: Do not use random feature sets but specific ones that are fitting the actual use-case to be tested (prevents flickering and improves testing time)
    //write features source
    putRandomFeatureCollectionToSpace(sourceSpaceBaseId, 7, xmin, ymin, xmax, ymax); // base will not be copied

    putRandomFeatureCollectionToSpace(sourceSpaceId, 20, xmin, ymin, xmax, ymax);                // v1
    putRandomFeatureCollectionToSpace(sourceSpaceId, 5, xmin, ymin, xmax, ymax);                 // v2
    putFeatureCollectionToSpace(sourceSpaceId, ftCollection);                                                 // v3
    putFeatureCollectionToSpace(sourceSpaceId, ftCollection2);                                                // v4
    deleteFeaturesInSpace(sourceSpaceId, List.of("id-deleted-feature","id-deleted-in-target-feature")); // v5
    putRandomFeatureCollectionToSpace(sourceSpaceId, 5, xmin, ymin, xmax, ymax);                 // v6
    putRandomFeatureCollectionToSpace(sourceSpaceId, 5, xmin, ymin, xmax, ymax);                 // v7
    putRandomFeatureCollectionToSpace(sourceSpaceId, 5, xmin, ymin, xmax, ymax);                 // v8

    //write features target - non-empty-space
    putRandomFeatureCollectionToSpace(targetSpaceId, 2, xmin, ymin, xmax, ymax);
    putFeatureCollectionToSpace(targetSpaceId, ftCollection2);

    putRandomFeatureCollectionToSpace(targetRemoteSpace, 2, xmin, ymin, xmax, ymax);
    putFeatureCollectionToSpace(targetRemoteSpace, ftCollection2);

  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpace(sourceSpaceId);
    deleteSpace(sourceSpaceBaseId);
    deleteSpace(targetSpaceId);
    deleteSpace(targetRemoteSpace);
  }

  private static Stream<Arguments> provideParameters() {
    return Stream.of(
        Arguments.of(false, null, false, null, null),
        Arguments.of(false, null, false, propertyFilter,null),
        Arguments.of(false, spatialSearchGeom, false, null,null),
        Arguments.of(false, spatialSearchGeom, true, null,null),
        Arguments.of(false, spatialSearchGeom, false, propertyFilter,null),
        Arguments.of(false, spatialSearchGeom, true, propertyFilter,null),

        Arguments.of(false, null, false, null, versionRange),
        Arguments.of(false, null, false, propertyFilter,versionRange),
        Arguments.of(false, spatialSearchGeom, false, null, versionRange),

        Arguments.of(true, null, false, null,null),
        Arguments.of(true, null, false, propertyFilter,null),
        Arguments.of(true, spatialSearchGeom, false, null,null),
        Arguments.of(true, spatialSearchGeom, true, null,null),
        Arguments.of(true, spatialSearchGeom, false, propertyFilter,null),
        Arguments.of(true, spatialSearchGeom, true, propertyFilter,null)
    );
  }

  @ParameterizedTest //(name = "{index}")
  @MethodSource("provideParameters")
  public void copySpace(boolean testRemoteDb, Geometry geo, boolean clip, String propertyFilter, String versionRef) throws Exception {
    Ref resolvedRef = versionRef == null ? new Ref(loadHeadVersion(sourceSpaceId)) : new Ref(versionRef);

    String targetSpace = !testRemoteDb ? CopySpaceStepsTest.targetSpaceId : targetRemoteSpace;

    StatisticsResponse statsBefore = getStatistics(targetSpace);

    assertEquals(NrFeaturesAtStartInTargetSpace, (Object) statsBefore.getCount().getValue());

    LambdaBasedStep step = new CopySpace()
        .withSpaceId(sourceSpaceId).withSourceVersionRef(resolvedRef)
        .withSpatialFilter( geo == null ? null : new SpatialFilter().withGeometry(geo).withClip(clip) )
        .withPropertyFilter(PropertiesQuery.fromString(propertyFilter))
        .withTargetSpaceId(targetSpace)
        .withJobId(JOB_ID)
        /* test only -> */.withTargetVersion(3); //TODO: rather provide the according model-based input instead (that would also directly test the functionality of providing the input accordingly)


    sendLambdaStepRequestBlock(step, true);

    long expectedCount = 43L;

    if( versionRef != null )
    { expectedCount = 12L;
      if( geo != null || propertyFilter != null ) //TODO: clarify - in case of filtering with versionRange, deleted features are not copied as they are not "found"
       expectedCount++;
    }

    StatisticsResponse statsAfter = getStatistics(targetSpace);
    assertEquals( expectedCount, (Object) statsAfter.getCount().getValue());
  }

  @Test
  public void copySpacePre() throws Exception {
    LambdaBasedStep step = new CopySpacePre()
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
    LambdaBasedStep step = new CopySpacePost()
        .withSpaceId(sourceSpaceId)
        .withJobId(JOB_ID);

    sendLambdaStepRequestBlock(step, true);

    List<Output> outputs = step.loadOutputs(USER);

    FeatureStatistics featureStatistics = null;

    for (Output output : outputs)
      if (output instanceof FeatureStatistics statistics)
        featureStatistics = statistics;

    assertTrue(featureStatistics != null && featureStatistics.getFeatureCount() == 0 && featureStatistics.getByteSize() == 0);
  }

  private static Stream<Arguments> provideCountParameters() {
    return Stream.of(
        Arguments.of( null, null, null, null), 
        Arguments.of( SpaceContext.DEFAULT, null, null, null), 
        Arguments.of( null, null, propertyFilter,null),
        Arguments.of( SpaceContext.DEFAULT, null, propertyFilter,null),
        Arguments.of( null, spatialSearchGeom, null,null),
        Arguments.of( SpaceContext.DEFAULT, spatialSearchGeom, null,null),
        Arguments.of( null, spatialSearchGeom, propertyFilter,null),
        Arguments.of( SpaceContext.DEFAULT, spatialSearchGeom, propertyFilter,null),
        
        Arguments.of( null, null, null, versionRange),
        Arguments.of( null, null, propertyFilter,versionRange),
        Arguments.of( null, spatialSearchGeom, null, versionRange) 
    );
  }


  @ParameterizedTest
  @MethodSource("provideCountParameters")
  public void countSpace(SpaceContext ctx, Geometry geo, String propertyFilter, String versionRef) throws Exception {

    assertTrue( versionRef == null || ctx == null || ctx == SpaceContext.EXTENSION ); // versionRef count is only supported in context EXTENSION

    Ref resolvedRef = versionRef == null ? new Ref(loadHeadVersion(sourceSpaceId)) : new Ref(versionRef);

    LambdaBasedStep step = new CountSpace()
        .withSpaceId(sourceSpaceId)
        .withSpatialFilter( geo == null ? null : new SpatialFilter().withGeometry(geo) )
        .withPropertyFilter(PropertiesQuery.fromString(propertyFilter))
        .withJobId(JOB_ID);
    
    ((CountSpace) step).setVersionRef(resolvedRef);
    ((CountSpace) step).setContext( ctx != null ? ctx : SpaceContext.EXTENSION );

    sendLambdaStepRequestBlock(step, true);

    List<Output> outputs = step.loadOutputs(USER);

    FeatureStatistics featureStatistics = null;

    for (Output output : outputs)
      if (output instanceof FeatureStatistics statistics)
        featureStatistics = statistics;

    long expectedCount = versionRef == null ? 40L : 12L; // assuming context EXTENSION
    
    if( ((CountSpace) step).getContext() == SpaceContext.DEFAULT ) // => no versionRef
     expectedCount = 47;

    if(versionRef != null && (geo != null || propertyFilter != null) ) //TODO: clarify - in case of filtering with versionRange, deleted features are not counted as they are not "found"
     expectedCount = 10;

    assertTrue(    featureStatistics != null 
                && featureStatistics.getFeatureCount() == expectedCount
                && featureStatistics.getByteSize() == 0);

  }


}
