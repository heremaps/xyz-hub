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

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;

public class CopySpaceStepsTestPsqlNc extends CopySpaceStepsTest {

  static {

   sourceSpaceId = "testCopy-drgnstn-Source-07";
   sourceSpaceBaseId = "testCopy-drgnstn-Source-base-07";
   targetSpaceId = "testCopy-drgnstn-Target-07";
   emptyTargetSpaceId = "testCopy-drgnstn-Target-07e";
   //???otherConnectorId = "psql_db2_hashed";
   targetRemoteSpace = "testCopy-drgnstn-Target-07-remote";
   emptyTargetRemoteSpace = "testCopy-drgnstn-Target-07e-remote";

  }

  private Map<String,Boolean> searchableProps = Map.of("$testAlias:[$.properties.test]::scalar", true );
  private ConnectorRef connector = new ConnectorRef().withId("psql-nl-connector");
  @Override
  protected void createSpaces()
  {
    createSpace(new Space().withId(sourceSpaceBaseId).withVersionsToKeep(100)
                                     .withStorage( connector )
                                     .withSearchableProperties(searchableProps)
                , false);

    createSpace(new Space().withId(sourceSpaceId).withVersionsToKeep(100)
                           //.withExtension(new Space.Extension().withSpaceId(sourceSpaceBaseId))
                           .withStorage( connector )
                           .withSearchableProperties(searchableProps)
                , false);

    createSpace(new Space().withId(targetSpaceId).withVersionsToKeep(100)
                           .withStorage( connector )
                           .withSearchableProperties(searchableProps)
                , false);

    createSpace(new Space().withId(targetRemoteSpace).withVersionsToKeep(100)
         /**??? */         .withStorage(new ConnectorRef().withId(otherConnectorId))
                           .withSearchableProperties(searchableProps)
                ,false);

    createSpace(new Space().withId(emptyTargetSpaceId)
                           .withVersionsToKeep(100)
                           .withStorage( connector )
                           .withSearchableProperties(searchableProps)
                , false);
    createSpace(new Space().withId(emptyTargetRemoteSpace)
                           .withVersionsToKeep(100)
          /**??? */        .withStorage(new ConnectorRef().withId(otherConnectorId))
                           .withSearchableProperties(searchableProps)
                ,false);
  }

  @Override
  @Disabled
  @Test
  public void copySpacePre() throws Exception {
  }

  private static Stream<Arguments> provideParameters() {
    return Stream.of(

        Arguments.of(false, null, false, null, null,false),

//        Arguments.of(false, null, false, null, null,true),

        Arguments.of(false, null, false, propertyFilter,null,false),
        Arguments.of(false, spatialSearchGeom, false, null,null,false),
        Arguments.of(false, spatialSearchGeom, true, null,null,false),
//        Arguments.of(false, spatialSearchGeom, true, null,null,true),
        Arguments.of(false, spatialSearchGeom, false, propertyFilter,null,false),
        Arguments.of(false, spatialSearchGeom, true, propertyFilter,null,false),

        Arguments.of(false, null, false, null, versionRange,false),
//        Arguments.of(false, null, false, null, versionRange,true),
        Arguments.of(false, null, false, propertyFilter,versionRange,false),
        Arguments.of(false, spatialSearchGeom, false, null, versionRange,false)

/*
        Arguments.of(true, null, false, null,null,false),
        Arguments.of(true, null, false, null,null,true),
        Arguments.of(true, null, false, propertyFilter,null,false),
        Arguments.of(true, spatialSearchGeom, false, null,null,false),
        Arguments.of(true, spatialSearchGeom, true, null,null,false),
        Arguments.of(true, spatialSearchGeom, true, null,null,true),
        Arguments.of(true, spatialSearchGeom, false, propertyFilter,null,false),
        Arguments.of(true, spatialSearchGeom, true, propertyFilter,null,false)
*/
    );
  }

  @Override
  @ParameterizedTest
  @MethodSource("provideParameters")
  public void copySpace(boolean testRemoteDb, Geometry geo, boolean clip, String propertyFilter, String versionRef, boolean emptyTarget) throws Exception {
    super.copySpace(testRemoteDb, geo, clip, propertyFilter, versionRef, emptyTarget);
  }


  @Override
  @Disabled
  @Test
  public void copySpacePost() throws Exception {
  }

  @Override
  @Disabled
  @ParameterizedTest
  @MethodSource("provideCountParameters")
  public void countSpace(SpaceContext ctx, Geometry geo, String propertyFilter, String versionRef) throws Exception {
  }

}
