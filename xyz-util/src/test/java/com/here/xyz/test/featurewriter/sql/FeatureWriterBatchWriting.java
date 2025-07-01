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

package com.here.xyz.test.featurewriter.sql;

import static com.here.xyz.test.featurewriter.SpaceWriter.DEFAULT_AUTHOR;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

public class FeatureWriterBatchWriting extends SQLTestSuite {
  private static final int BATCH_SIZE = 10000;

  private List<Feature> generateLargeRandomBatch(int featureCount) {
    List<Feature> features = new ArrayList<>();
    for (int i = 0; i < featureCount; i++)
      features.add(new Feature()
          .withProperties(new Properties().with("test", i))
          .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0, 0))));
    return features;
  }

  @Disabled
  @CartesianTest
  public void writeLargeBatch(@Values(ints = {1, 2}) int runAttempt) throws Exception {
    SQLSpaceWriter spaceWriter = ((SQLSpaceWriter) spaceWriter());
    spaceWriter.writeFeatures(generateLargeRandomBatch(BATCH_SIZE), DEFAULT_AUTHOR, OnExists.REPLACE, OnNotExists.CREATE, null,
        OnMergeConflict.REPLACE, false, SpaceContext.DEFAULT, true);
  }
}
