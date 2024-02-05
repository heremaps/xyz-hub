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
import static com.here.xyz.psql.query.branching.BranchManager.headRef;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.branching.BranchManager.MergeOperationResult;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BMMerge extends BMBase {

  @Test
  public void mergeOnlyOnMain() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      BranchManager bm = branchManager(dsp);

      writeFeature(newTestFeature("f1"), REF_MAIN_HEAD);
      writeFeature(newTestFeature("f2.main"), REF_MAIN_HEAD);

      Ref branch1Base = Ref.fromBranchId(REF_MAIN, 1);
      Ref branch1Ref = createBranch(branch1Base);

      writeFeature(newTestFeature("f2.b1"), branch1Ref);

      //Execute the merge
      MergeOperationResult result = bm.merge(getNodeId(branch1Ref), branch1Base, MAIN_NODE_ID, false);
      assertMergeSuccess(result, branch1Ref, branch1Base, 2, Ref.fromBranchId(REF_MAIN, 3));

      //Ensure branch1 remained unchanged
      assertFeatureNotExists("f2.main", branch1Ref);
      for (String id : List.of("f1", "f2.b1"))
        assertFeatureExists(id, branch1Ref);

      //Check if the merge was performed properly
      for (String id : List.of("f1", "f2.main", "f2.b1"))
        assertFeatureExists(id, REF_MAIN_HEAD);
    }
  }

  @Test
  public void mergeOnlyOnMainWithSolvableConflicts() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      BranchManager bm = branchManager(dsp);

      writeFeature(newTestFeature("f1"), REF_MAIN_HEAD);
      writeFeature(newTestFeature("f1").withProperties(new Properties().with("A", "valueA")), REF_MAIN_HEAD);

      Ref branch1Base = Ref.fromBranchId(REF_MAIN, 1);
      Ref branch1Ref = createBranch(branch1Base);

      writeFeature(newTestFeature("f1").withProperties(new Properties().with("B", "valueB")), branch1Ref);

      //Execute the merge
      MergeOperationResult result = bm.merge(getNodeId(branch1Ref), branch1Base, MAIN_NODE_ID, false);
      assertMergeSuccess(result, branch1Ref, branch1Base, 2, Ref.fromBranchId(REF_MAIN, 3));

      //Ensure branch1 remained unchanged
      assertNull(readFeatureById("f1", branch1Ref).getProperties().get("A"));
      assertEquals("valueB", readFeatureById("f1", branch1Ref).getProperties().get("B"));

      //Check if the merge was performed properly
      assertEquals("valueA", readFeatureById("f1", REF_MAIN_HEAD).getProperties().get("A"));
      assertEquals("valueB", readFeatureById("f1", REF_MAIN_HEAD).getProperties().get("B"));
    }
  }

  @Test
  public void mergeOnlyOnMainWithNonSolvableConflicts() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      BranchManager bm = branchManager(dsp);

      writeFeature(newTestFeature("f1"), REF_MAIN_HEAD);
      writeFeature(newTestFeature("f1").withProperties(new Properties().with("A", "valueA")), REF_MAIN_HEAD);

      Ref branch1Base = Ref.fromBranchId(REF_MAIN, 1);
      Ref branch1Ref = createBranch(branch1Base);

      writeFeature(newTestFeature("f1").withProperties(new Properties().with("A", "valueB")), branch1Ref);

      //Execute the merge
      MergeOperationResult result = bm.merge(getNodeId(branch1Ref), branch1Base, MAIN_NODE_ID, false);
      assertMergeConflicts(result, List.of(MAIN_NODE_ID, getNodeId(branch1Ref)), branch1Base, 2,
          Ref.fromBranchId(REF_MAIN, 3));
      Ref tmpBranchRef = headRef(result.nodeId());

      //Ensure branch1 remained unchanged
      assertEquals("valueB", readFeatureById("f1", branch1Ref).getProperties().get("A"));

      //Check if the merge was performed properly and the conflicting features were marked as such
      Feature conflictingFeature = readFeatureById("f1", tmpBranchRef);
      assertEquals("valueB", conflictingFeature.getProperties().get("A"));
      assertEquals(true, conflictingFeature.getProperties().getXyzNamespace().isConflicting());
    }
  }








  private static void assertMergeSuccess(MergeOperationResult result, Ref branchRef, Ref branchBaseRef, long expectedMergedSourceVersion, Ref expectedResolvedMergeTargetRef) {
    assertFalse(result.conflicting());
    assertEquals(getNodeId(branchRef), result.nodeId());
    assertEquals(branchBaseRef, result.baseRef());
    assertEquals(expectedMergedSourceVersion, result.mergedSourceVersion());
    assertEquals(expectedResolvedMergeTargetRef, result.resolvedMergeTargetRef());
  }

  private static void assertMergeConflicts(MergeOperationResult result, List<Integer> notAllowedNodeIds, Ref branchBaseRef, long expectedMergedSourceVersion, Ref expectedResolvedMergeTargetRef) {
    assertTrue(result.conflicting());
    //In the case of a conflicting merge the resulting nodeId is a new temporary one
    notAllowedNodeIds.forEach(notAllowedNodeId -> assertNotEquals(notAllowedNodeId, result.nodeId()));
    assertNotEquals(branchBaseRef, result.baseRef());
    assertEquals(expectedMergedSourceVersion, result.mergedSourceVersion());
    assertEquals(expectedResolvedMergeTargetRef, result.resolvedMergeTargetRef());
  }
}
