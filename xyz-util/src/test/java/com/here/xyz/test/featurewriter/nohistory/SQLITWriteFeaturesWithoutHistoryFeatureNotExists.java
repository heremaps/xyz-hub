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

package com.here.xyz.test.featurewriter.nohistory;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.featurewriter.SQLITWriteFeaturesBase;
import org.junit.Test;

public class SQLITWriteFeaturesWithoutHistoryFeatureNotExists extends SQLITWriteFeaturesBase {

  //********************** Feature not exists (OnVersionConflict deactivated) *******************************/
  @Test
  public void writeToNotExistingFeature_OnNotExistsCREATE() throws Exception {
    insertTestFeature(null, OnNotExists.CREATE, null);
    checkExistingFeature(createTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToNotExistingFeature_OnNotExistsERROR() throws Exception {
    insertTestFeature(null, OnNotExists.ERROR, SQLErrorCodes.XYZ44);
    checkNotExistingFeature(createTestFeatures().get(0));
  }

  @Test
  public void writeToNotExistingFeature_OnNotExistsRETAIN() throws Exception {
    insertTestFeature(null, OnNotExists.RETAIN,null);
    checkNotExistingFeature(createTestFeatures().get(0));
  }

  //********************** Feature not exists (OnVersionConflict.REPLACE) *******************************/
  @Test
  public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsCREATE() throws Exception {
    writeFeature(1L,false, null , OnNotExists.CREATE, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createModifiedTestFeatures().get(0), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsERROR() throws Exception {
    writeFeature(1L,false, null , OnNotExists.ERROR, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ45);

    checkNotExistingFeature(createModifiedTestFeatures().get(0));
  }

  @Test
  public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsRETAIN() throws Exception {
    writeFeature(1L,false, null , OnNotExists.RETAIN, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(createModifiedTestFeatures().get(0));
  }
}
