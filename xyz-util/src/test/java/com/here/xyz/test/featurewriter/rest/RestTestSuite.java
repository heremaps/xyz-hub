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

package com.here.xyz.test.featurewriter.rest;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.test.featurewriter.TestSuite;

public abstract class RestTestSuite extends TestSuite {

  @Override
  protected SpaceWriter spaceWriter() {
    return new RestSpaceWriter(composite, history, getClass().getSimpleName());
  }

  /**
   * Preparation writes must go through the Hub in the REST test suites, because the physical
   * table name is opaque to the tests (see {@code SpaceTableResolver}). Writing directly via
   * {@code SQLSpaceWriter} would target a table named after the space id, which may not exist.
   */
  @Override
  protected void writeFeatureForPreparation(Feature feature, String author, SpaceContext context) throws Exception {
    spaceWriter().writeFeature(feature, author, null, null, null, null, false, context, history);
    // Keep the base-version bookkeeping consistent with the parent implementation.
    writtenSpaceVersions.get(context).increment();
  }

  public RestTestSuite() {
    /*
    TODO: Hub currently only supports to define the author through JWT or request header.
     Improve featurewriter tests to set author through JWT as the rest of the tests are doing it, then remove this workaround.
     */
    applyAuthorWorkaround = true;
  }
}
