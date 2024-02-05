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

package com.here.xyz.psql.query.branching;

import static com.here.xyz.psql.query.branching.BranchManager.getNodeId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.branching.BranchManager.BranchOperationResult;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BMRebase extends BMBase {

  private BranchManager bm;

  //TODO: Allow to use "HEAD" ref in CommitManager?
  @Test
  public void rebaseOnlyOnMain() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      bm = branchManager(dsp);

      writeFeature(newTestFeature("f1"), REF_MAIN_HEAD);
      writeFeature(newTestFeature("f2.main"), REF_MAIN_HEAD);

      Ref branch1Base = Ref.fromBranchId(REF_MAIN, 1);
      Ref branch1Ref = createBranch(branch1Base);

      writeFeature(newTestFeature("f2.b1"), branch1Ref);

      //Execute the rebase
      BranchOperationResult result = bm.rebase(getNodeId(branch1Ref), branch1Base, REF_MAIN_HEAD);
      assertRebaseSuccess(result, branch1Ref, REF_MAIN_HEAD);
      Ref rebasedBranch1Ref = branchRefOf(result.nodeId());

      //Ensure branch1 remained unchanged
      assertFeatureNotExists("f2.main", branch1Ref);
      for (String id : List.of("f1", "f2.b1"))
        assertFeatureExists(id, branch1Ref);

      //Check if the rebase was performed properly
      assertFeatureNotExists("f2.b1", REF_MAIN_HEAD);
      for (String id : List.of("f1", "f2.main", "f2.b1"))
        assertFeatureExists(id, rebasedBranch1Ref);
    }
  }

  @Test
  public void rebaseOnlyOnMainWithSolvableConflicts() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      bm = branchManager(dsp);

      writeFeature(newTestFeature("f1"), REF_MAIN_HEAD);
      writeFeature(newTestFeature("f1").withProperties(new Properties().with("A", "valueA")), REF_MAIN_HEAD);

      Ref branch1Base = Ref.fromBranchId(REF_MAIN, 1);
      Ref branch1Ref = createBranch(branch1Base);

      writeFeature(newTestFeature("f1").withProperties(new Properties().with("B", "valueB")), branch1Ref);

      //Execute the rebase
      BranchOperationResult result = bm.rebase(getNodeId(branch1Ref), branch1Base, REF_MAIN_HEAD);
      assertRebaseSuccess(result, branch1Ref, REF_MAIN_HEAD);
      Ref rebasedBranch1Ref = branchRefOf(result.nodeId());

      //Ensure branch1 remained unchanged
      assertNull(readFeatureById("f1", branch1Ref).getProperties().get("A"));
      assertEquals("valueB", readFeatureById("f1", branch1Ref).getProperties().get("B"));

      //Check if the rebase was performed properly
      Feature affectedFeature = readFeatureById("f1", rebasedBranch1Ref);
      assertEquals("valueA", affectedFeature.getProperties().get("A"));
      assertEquals("valueB", affectedFeature.getProperties().get("B"));
    }
  }

  @Test
  public void rebaseOnlyOnMainWithNonSolvableConflicts() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      bm = branchManager(dsp);

      writeFeature(newTestFeature("f1"), REF_MAIN_HEAD);
      writeFeature(newTestFeature("f1").withProperties(new Properties().with("A", "valueA")), REF_MAIN_HEAD);

      Ref branch1Base = Ref.fromBranchId(REF_MAIN, 1);
      Ref branch1Ref = createBranch(branch1Base);

      writeFeature(newTestFeature("f1").withProperties(new Properties().with("A", "valueB")), branch1Ref);

      //Execute the rebase
      BranchOperationResult result = bm.rebase(getNodeId(branch1Ref), branch1Base, REF_MAIN_HEAD);
      assertRebaseConflicts(result, branch1Ref, REF_MAIN_HEAD);
      Ref rebasedBranch1Ref = branchRefOf(result.nodeId());

      //Ensure branch1 remained unchanged
      assertEquals("valueB", readFeatureById("f1", branch1Ref).getProperties().get("A"));

      //Check if the rebase was performed properly and the conflicting features were marked as such
      Feature conflictingFeature = readFeatureById("f1", rebasedBranch1Ref);
      assertEquals("valueB", conflictingFeature.getProperties().get("A"));
      assertEquals(true, conflictingFeature.getProperties().getXyzNamespace().isConflicting());
    }
  }


  /*
  TODO: Add the following tests:

  - rebaseOnlyOnMain with expected conflicts

  - rebaseMultipleCommitsOnlyOnMain()
  - rebaseMultipleCommitsOnlyOnMain() with expected conflicts

  starting from here, always with multiple commits (2 commits)
  - rebaseOnlyOnSameBaseBranch()
  - rebaseOnlyOnSameBaseBranch() with expected conflicts
  - rebaseOnOtherBranch
  - rebaseOnOtherBranch with expected conflicts
   */

  private void assertRebaseSuccess(BranchOperationResult result, Ref branchRef, Ref newBaseRef) throws SQLException {
    assertFalse(result.conflicting());
    checkNewBranchOnRebaseComplete(result, branchRef, newBaseRef);
  }

  private void assertRebaseConflicts(BranchOperationResult result, Ref branchRef, Ref newBaseRef) throws SQLException {
    assertTrue(result.conflicting());
    checkNewBranchOnRebaseComplete(result, branchRef, newBaseRef);
  }

  private void checkNewBranchOnRebaseComplete(BranchOperationResult result, Ref branchRef, Ref newBaseRef) throws SQLException {
    assertNotEquals(getNodeId(branchRef), result.nodeId());
    assertEquals(bm.resolveHead(newBaseRef), result.baseRef());
  }
}
