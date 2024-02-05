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

import com.here.xyz.models.hub.Ref;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BMBranching extends BMBase {

  @Test
  public void writeFeatureToBranch() throws Exception {
    writeFeature(newTestFeature("f1"), REF_MAIN_HEAD);
    writeFeature(newTestFeature("f2.main"), REF_MAIN_HEAD);

    Ref branch1Base = Ref.fromBranchId(REF_MAIN, 1);
    Ref branch1Ref = createBranch(branch1Base);

    writeFeature(newTestFeature("f2.b1"), branch1Ref);

    //Check contents of main branch
    assertFeatureNotExists("f2.b1", REF_MAIN_HEAD);
    for (String id : List.of("f1", "f2.main"))
      assertFeatureExists(id, REF_MAIN_HEAD);

    //Check contents of branch1
    assertFeatureNotExists("f2.main", branch1Ref);
    for (String id : List.of("f1", "f2.b1"))
      assertFeatureExists(id, branch1Ref);
  }
}
