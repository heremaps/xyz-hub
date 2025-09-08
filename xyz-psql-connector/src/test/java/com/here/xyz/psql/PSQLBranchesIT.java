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

package com.here.xyz.psql;

import static com.here.xyz.events.ModifyBranchEvent.Operation.CREATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.here.xyz.events.ModifyBranchEvent;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.responses.ModifiedBranchResponse;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PSQLBranchesIT extends PSQLAbstractBranchIT {

  @BeforeEach
  public void setup() throws Exception {
    invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
  }

  @AfterEach
  public void shutdown() throws Exception {
    invokeDeleteTestSpace();
  }

  @Test
  public void createBranchOnMainHead() throws Exception {

    ModifyBranchEvent mbe = eventForCreate(CREATE, getBaseRef(0));

    ModifiedBranchResponse res1 = deserializeResponse(invokeLambda(mbe));
    assertEquals(1, res1.getNodeId());
    assertTrue(checkIfBranchTableExists(1, 0, 0));

    ModifiedBranchResponse res2 = deserializeResponse(invokeLambda(mbe));
    assertEquals(2, res2.getNodeId());
    assertTrue(checkIfBranchTableExists(2, 0, 0));

    ModifiedBranchResponse res3 = deserializeResponse(invokeLambda(mbe));
    assertEquals(3, res3.getNodeId());
    assertTrue(checkIfBranchTableExists(3, 0, 0));
  }

  @Test
  public void createBranchOnMainVersion() throws Exception {

    long v1 = extractVersion(deserializeResponse(writeFeature("f1")));
    long v2 = extractVersion(deserializeResponse(writeFeature("f2")));
    long v3 = extractVersion(deserializeResponse(writeFeature("f3")));

    ModifiedBranchResponse res1 = deserializeResponse(invokeLambda(eventForCreate(CREATE, getBaseRef(0, v1))));
    assertEquals(1, res1.getNodeId());
    assertEquals(getBaseRef(0, v1).toString(), res1.getBaseRef().toString());
    assertTrue(checkIfBranchTableExists(1, 0, v1));

    ModifiedBranchResponse res2 = deserializeResponse(invokeLambda(eventForCreate(CREATE, getBaseRef(0, v2))));
    assertEquals(2, res2.getNodeId());
    assertEquals(getBaseRef(0, v2).toString(), res2.getBaseRef().toString());
    assertTrue(checkIfBranchTableExists(2, 0, v2));

    ModifiedBranchResponse res3 = deserializeResponse(invokeLambda(eventForCreate(CREATE, getBaseRef(0, v3))));
    assertEquals(3, res3.getNodeId());
    assertEquals(getBaseRef(0, v3).toString(), res3.getBaseRef().toString());
    assertTrue(checkIfBranchTableExists(3, 0, v3));

  }

  @Test
  public void createBranchOnBranch() throws Exception {
    int baseNodeId = 0;

    //Add Feature to main, version=1
    long main_v1 = extractVersion(deserializeResponse(writeFeature("main_1")));

    //Create branch b1 on main version 1
    Ref b1_baseRef = getBaseRef(baseNodeId, main_v1);
    ModifiedBranchResponse b1_main = deserializeResponse(invokeLambda(eventForCreate(CREATE, b1_baseRef)));
    assertEquals(b1_baseRef, b1_main.getBaseRef());
    assertTrue(checkIfBranchTableExists(b1_main.getNodeId(), baseNodeId, main_v1));

    //Add feature to branch b1, version=2
    long b1_v2 = extractVersion(deserializeResponse(writeFeature("b1_1", b1_main.getNodeId(), List.of(b1_baseRef))));

    //Create branch b2 on branch b1 version 2
    Ref b2_baseRef = getBaseRef(b1_main.getNodeId(), b1_v2);
    ModifiedBranchResponse b2_b1 = deserializeResponse(invokeLambda(eventForCreate(CREATE, b2_baseRef)));
    assertEquals(b2_baseRef, b2_b1.getBaseRef());
    assertTrue(checkIfBranchTableExists(b2_b1.getNodeId(), b1_main.getNodeId(), b1_v2));

    //Add feature to branch b2, version=3
    long b2_v3 = extractVersion(deserializeResponse(writeFeature("b2_1", b2_b1.getNodeId(), List.of(b1_baseRef,b2_baseRef))));

    //Create branch b3 on branch b2 version 3
    Ref b3_baseRef = getBaseRef(b2_b1.getNodeId(), b2_v3);
    ModifiedBranchResponse b3_b2 = deserializeResponse(invokeLambda(eventForCreate(CREATE, b3_baseRef)));
    assertEquals(b3_baseRef, b3_b2.getBaseRef());
    assertTrue(checkIfBranchTableExists(b3_b2.getNodeId(), b2_b1.getNodeId(), b2_v3));

  }


}