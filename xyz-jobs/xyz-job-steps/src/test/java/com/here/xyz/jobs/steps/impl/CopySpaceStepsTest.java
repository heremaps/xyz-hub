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

package com.here.xyz.jobs.steps.impl;

import com.here.xyz.jobs.steps.execution.LambdaBasedStep;
import com.here.xyz.jobs.steps.impl.transport.CopySpace;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.responses.StatisticsResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.stream.Stream;

public class CopySpaceStepsTest extends StepTest {

  static private String SrcSpc    = "testCopy-Source-07", 
                        TrgSpc    = "testCopy-Target-07",
                        OtherCntr = "psql_db2_hashed",
                        TrgRmtSpc = "testCopy-Target-07-remote",
                        propertyFilter = "p.all=common";
         
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
   spatialSearchGeom = new Polygon().withCoordinates( pc );

  }

  @BeforeEach
  public void setup() throws SQLException {
      cleanup();
      createSpace(new Space().withId(SrcSpc).withVersionsToKeep(100),false);
      createSpace(new Space().withId(TrgSpc).withVersionsToKeep(100),false);
      createSpace(new Space().withId(TrgRmtSpc).withVersionsToKeep(100).withStorage(new ConnectorRef().withId(OtherCntr)),false);

      //write features source
      putRandomFeatureCollectionToSpace(SrcSpc, 20,xmin,ymin,xmax,ymax);
      putRandomFeatureCollectionToSpace(SrcSpc, 20,xmin,ymin,xmax,ymax);
      //write features target - non-empty-space
      putRandomFeatureCollectionToSpace(TrgSpc, 2,xmin,ymin,xmax,ymax);

      putRandomFeatureCollectionToSpace(TrgRmtSpc, 2,xmin,ymin,xmax,ymax);

  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpace(SrcSpc);
    deleteSpace(TrgSpc);
    deleteSpace(TrgRmtSpc);
  }

  private static Stream<Arguments> provideParameters() {
    return Stream.of(
        Arguments.of(false,null,false,null),
        Arguments.of(false,null,false,propertyFilter),
        Arguments.of(false, spatialSearchGeom,false,null),
        Arguments.of(false, spatialSearchGeom,true,null),
        Arguments.of(false, spatialSearchGeom,false,propertyFilter),
        Arguments.of(false, spatialSearchGeom,true,propertyFilter),
        
        Arguments.of(true,null,false,null),
        Arguments.of(true,null,false,propertyFilter),
        Arguments.of(true, spatialSearchGeom,false,null),
        Arguments.of(true, spatialSearchGeom,true,null),
        Arguments.of(true, spatialSearchGeom,false,propertyFilter),
        Arguments.of(true, spatialSearchGeom,true,propertyFilter)
    );
  }

@ParameterizedTest //(name = "{index}")
@MethodSource("provideParameters")
  public void testCopySpaceToSpaceStep( boolean testRemoteDb, Geometry geo, boolean clip, String propertyFilter) throws Exception {

    String targetSpace = !testRemoteDb ? TrgSpc : TrgRmtSpc;
    
    StatisticsResponse statsBefore = getStatistics(targetSpace);

    assertEquals(2L, (Object) statsBefore.getCount().getValue());

    LambdaBasedStep step = new CopySpace()
                               .withSpaceId(SrcSpc).withSourceVersionRef(new Ref("HEAD"))
                               .withGeometry( geo ).withClipOnFilterGeometry(clip)
                               .withPropertyFilter(propertyFilter)
                               .withTargetSpaceId( targetSpace );
          
    sendLambdaStepRequestBlock(step,  true);

    StatisticsResponse statsAfter = getStatistics(targetSpace);
    assertEquals(42L, (Object) statsAfter.getCount().getValue());
 }

}
