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

import java.util.Arrays;

public class SQLITWriteFeaturesWithoutHistoryFeatureNotExists extends SQLITWriteFeaturesBase {

  //********************** Feature not exists (OnVersionConflict deactivated) *******************************/
  @Test
  public void writeToNotExistingFeature_OnNotExistsCREATE() throws Exception {
    runWriteFeatureQueryWithSQLAssertion(Arrays.asList(createTestFeature(null)), author, null ,  OnNotExists.CREATE,
            null, null, false, SpaceContext.EXTENSION,false, null);
    checkExistingFeature(createTestFeature(null), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToNotExistingFeature_OnNotExistsERROR() throws Exception {
    runWriteFeatureQueryWithSQLAssertion(Arrays.asList(createTestFeature(null)), author, null ,  OnNotExists.ERROR,
            null, null, false, SpaceContext.EXTENSION,false, SQLErrorCodes.XYZ44);
    checkNotExistingFeature(createTestFeature(null));
  }

  @Test
  public void writeToNotExistingFeature_OnNotExistsRETAIN() throws Exception {
    runWriteFeatureQueryWithSQLAssertion(Arrays.asList(createTestFeature(null)), author, null ,  OnNotExists.RETAIN,
            null, null, false, SpaceContext.EXTENSION,false, null);
    checkNotExistingFeature(createTestFeature(null));
  }

  //********************** Feature not exists (OnVersionConflict.REPLACE) *******************************/
  @Test
  public void writeToNotExistingFeature_WithConflictHandling_WithoutBaseVersion() throws Exception {
    writeFeature(Arrays.asList(createTestFeature(null)), null , OnNotExists.CREATE, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ40);

    checkNotExistingFeature(createTestFeature(null));
  }

  @Test
  public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsCREATE() throws Exception {
    writeFeature(Arrays.asList(createTestFeature(1L)), null , OnNotExists.CREATE, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(createTestFeature(1l), 1L, Long.MAX_VALUE, Operation.I, author);
  }

  @Test
  public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsERROR() throws Exception {
    writeFeature(Arrays.asList(createTestFeature(1L)), null , OnNotExists.ERROR, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, SQLErrorCodes.XYZ45);

    checkNotExistingFeature(createTestFeature(null));
  }

  @Test
  public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsRETAIN() throws Exception {
    writeFeature(Arrays.asList(createTestFeature(1L)), null , OnNotExists.RETAIN, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(createTestFeature(null));
  }
}
