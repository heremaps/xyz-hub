/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.xyz.psql;

import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.changesets.ChangesetCollection;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;



public class PSQLChangesets extends PSQLAbstractIT {

  public static String SPACE_ID = String.format("%s%d",PSQLChangesets.class.getSimpleName().toLowerCase(),ProcessHandle.current().pid()),
                       FMT_AUTHOR = "Author%d";

  @BeforeEach
  public void createTable() throws Exception {
    invokeCreateTestSpace(defaultTestConnectorParams, SPACE_ID);
    writeFeatures();
  }

  @AfterEach
  public void shutdown() throws Exception {
    invokeDeleteTestSpaces(null, List.of(SPACE_ID));
  }


  public static long TimeOffset = 1000,
                      StartTime = TimeOffset + 4,
                      EndTime   = TimeOffset + 7;
  public static String LookUpAuthor = String.format(FMT_AUTHOR, 2 );

  public static void writeFeatures() throws Exception {

    for( int i = 0; i < 10; i++)
    {
     long ts = i + TimeOffset;
     String author = String.format(FMT_AUTHOR, (i % 2)+1 );
     XyzNamespace xyzNamespace = new XyzNamespace().withSpace(SPACE_ID).withCreatedAt(ts).withUpdatedAt(ts).withAuthor(author);
     ModifyFeaturesEvent mfe = new ModifyFeaturesEvent()
            .withConnectorParams(defaultTestConnectorParams)
            .withSpace(SPACE_ID)
            .withTransaction(true)
            .withConflictDetectionEnabled(false)
            .withVersionsToKeep(100)
            .withAuthor(author)
            .withUpsertFeatures(Arrays.asList(
                    new Feature().withId("F1").withProperties(new Properties().withXyzNamespace(xyzNamespace).with("strValue",String.format("x%d",i))),
                    new Feature().withId("F2").withProperties(new Properties().withXyzNamespace(xyzNamespace).with("strValue",String.format("x%d",i)))
            ));
     invokeLambda(mfe);
    }
  }

  private static Stream<Arguments> provideParameters() {
    return Stream.of(
        Arguments.of(StartTime, 0, null, 5,6,10),
        Arguments.of(0, EndTime, null, 8,1,8),
        Arguments.of(0, 0, LookUpAuthor, 5,2,10),

        Arguments.of(StartTime, EndTime, null, 3,6,8),
        Arguments.of(StartTime, 0 , LookUpAuthor, 3,6,10),
        Arguments.of( 0 , EndTime, LookUpAuthor, 4,2,8),

        Arguments.of(StartTime, EndTime, LookUpAuthor, 2,6,8)
    );
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void iterateChangesets(long startTime, long endTime, String author, int eSize, int eStartVer, int eEndVer) throws Exception {

    IterateChangesetsEvent ice =
     new IterateChangesetsEvent()
            .withStreamId("iterateByTimeRange")
            .withSpace(SPACE_ID)
            .withLimit(50)
            .withRef( new Ref(new Ref(0), new Ref(Ref.HEAD) ) );

    if( startTime > 0 )
     ice.setStartTime(startTime);

    if( endTime > 0 )
     ice.setEndTime(endTime);

    if( author != null && !author.isEmpty())
     ice.setAuthors(List.of(author,"NonExistingAuthor"));

    String response = invokeLambda(ice);
    ChangesetCollection changesetCollection =  deserializeResponse(response);

    assertEquals(eSize, changesetCollection.getVersions().size() );
    assertEquals(eStartVer, changesetCollection.getStartVersion() );
    assertEquals(eEndVer, changesetCollection.getEndVersion() );
  }

/*
  @Test
  public void doIterateChangesets() throws Exception {

    long startTime = StartTime, endTime = EndTime;
    String author = LookUpAuthor;
    int eSize = 2, eStartVer = 6, eEndVer = 8;

    IterateChangesetsEvent ice =
     new IterateChangesetsEvent()
            .withStreamId("iterateByTimeRange")
            .withSpace(SPACE_ID)
            .withLimit(50)
            .withRef( new Ref(new Ref(0), new Ref(Ref.HEAD) ) );

    if( startTime > 0 )
     ice.setStartTime(startTime);

    if( endTime > 0 )
     ice.setEndTime(endTime);

    if( author != null && !author.isEmpty())
     ice.setAuthors(List.of(author,"NonExistingAuthor"));

    String response = invokeLambda(ice);
    ChangesetCollection changesetCollection =  deserializeResponse(response);

    assertEquals(eSize, changesetCollection.getVersions().size() );
    assertEquals(eStartVer, changesetCollection.getStartVersion() );
    assertEquals(eEndVer, changesetCollection.getEndVersion() );
  }
*/

}
