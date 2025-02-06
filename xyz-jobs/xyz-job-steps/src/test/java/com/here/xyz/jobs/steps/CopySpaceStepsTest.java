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

import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.StepTest;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePost;
import com.here.xyz.jobs.steps.impl.transport.CopySpacePre;
import com.here.xyz.jobs.steps.outputs.CreatedVersion;
import com.here.xyz.jobs.steps.outputs.FeatureStatistics;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
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

  static private String sourceSpace = "testCopy-Source-07",
      targetSpace = "testCopy-Target-07",
      otherConnector = "psql_db2_hashed",
      targetRemoteSpace = "testCopy-Target-07-remote",
      propertyFilter = "p.all=common",
      versionRange = "1..2";

  static private Polygon spatialSearchGeom;
  static private float xmin = 7.0f, ymin = 50.0f, xmax = 7.1f, ymax = 50.1f;

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
  }

  @BeforeEach
  public void setup() throws SQLException {
    cleanup();
    createSpace(new Space().withId(sourceSpace).withVersionsToKeep(100), false);
    createSpace(new Space().withId(targetSpace).withVersionsToKeep(100), false);
    createSpace(new Space().withId(targetRemoteSpace).withVersionsToKeep(100).withStorage(new ConnectorRef().withId(otherConnector)),
        false);

    //write features source
    putRandomFeatureCollectionToSpace(sourceSpace, 20, xmin, ymin, xmax, ymax); // v1
    putRandomFeatureCollectionToSpace(sourceSpace, 10, xmin, ymin, xmax, ymax); // v2
    putRandomFeatureCollectionToSpace(sourceSpace, 10, xmin, ymin, xmax, ymax); // v3

    //write features target - non-empty-space
    putRandomFeatureCollectionToSpace(targetSpace, 2, xmin, ymin, xmax, ymax);
    putRandomFeatureCollectionToSpace(targetRemoteSpace, 2, xmin, ymin, xmax, ymax);

  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpace(sourceSpace);
    deleteSpace(targetSpace);
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
    String targetSpace = !testRemoteDb ? CopySpaceStepsTest.targetSpace : targetRemoteSpace;

    StatisticsResponse statsBefore = getStatistics(targetSpace);

    assertEquals(2L, (Object) statsBefore.getCount().getValue());

    LambdaBasedStep step = new CopySpace()
        .withSpaceId(sourceSpace).withSourceVersionRef(new Ref(versionRef == null ? "HEAD" : versionRef ))
        .withGeometry(geo).withClipOnFilterGeometry(clip)
        .withPropertyFilter(PropertiesQuery.fromString(propertyFilter))
        .withTargetSpaceId(targetSpace)
        .withJobId(JOB_ID);

    sendLambdaStepRequestBlock(step, true);

    StatisticsResponse statsAfter = getStatistics(targetSpace);
    assertEquals( versionRef == null ? 42L : 12L, (Object) statsAfter.getCount().getValue());
  }

  @Test
  public void copySpacePre() throws Exception {
    LambdaBasedStep step = new CopySpacePre()
        .withSpaceId(targetSpace)
        .withJobId(JOB_ID);

    sendLambdaStepRequestBlock(step, true);

    List<?> outputs = step.loadOutputs(SYSTEM);

    long fetchedVersion = -1;
    for (Object output : outputs)
      if (output instanceof CreatedVersion f)
        fetchedVersion = f.getVersion();

    assertEquals(2l, fetchedVersion);
  }

  @Test
  public void copySpacePost() throws Exception {
    LambdaBasedStep step = new CopySpacePost()
        .withSpaceId(sourceSpace)
        .withJobId(JOB_ID);

    sendLambdaStepRequestBlock(step, true);

    List<Output> outputs = step.loadOutputs(USER);

    FeatureStatistics featureStatistics = null;

    for (Output output : outputs)
      if (output instanceof FeatureStatistics statistics)
        featureStatistics = statistics;

    assertTrue(featureStatistics != null && featureStatistics.getFeatureCount() == 0 && featureStatistics.getByteSize() == 0);
  }

}
