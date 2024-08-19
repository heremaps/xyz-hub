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

package com.here.xyz.test.featurewriter._custom;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import org.junit.Test;

public class SQLITWriteFeaturesWithoutHistoryDefaults extends SQLITWriteFeaturesBase {

  public SQLITWriteFeaturesWithoutHistoryDefaults() {
    super(false);
  }

  @Test
  public void writeFeature_WithDefaults() throws Exception {
    Feature f1 = XyzSerializable.deserialize("""
        { "type":"Feature",
          "id":"id1",
          "geometry":{"type":"Point","coordinates":[8.0,50.0]},
          "properties":{"firstName":"Alice","age":35}
        }
        """, Feature.class);

    writeFeature(f1, DEFAULT_AUTHOR, null, null,
        null, null, false, SpaceContext.EXTENSION, false);

    checkExistingFeature(f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_WithDefaults() throws Exception {
    //initial write
    Feature f1 = XyzSerializable.deserialize("""
        { "type":"Feature",
          "id":"id1",
          "geometry":{"type":"Point","coordinates":[8.0,50.0]},
          "properties":{"firstName":"Alice","age":35}
        }
        """, Feature.class);

    writeFeature(f1, DEFAULT_AUTHOR, null, null,
        null, null, false, SpaceContext.EXTENSION, false);

    //second write || Default=REPLACE
    Feature f2 = XyzSerializable.deserialize("""
        { "type":"Feature",
          "id":"id1",
          "geometry":{"type":"Point","coordinates":[8.0,50.0]},
          "properties":{"lastName":"Wonder"}
        }
        """, Feature.class);
    writeFeature(f2, UPDATE_AUTHOR, null, null, null, null,
        false, SpaceContext.EXTENSION, false);

    checkExistingFeature(f2, 2L, Long.MAX_VALUE, Operation.U, UPDATE_AUTHOR);
  }

  @Test
  public void writeTiExistingFeature_WithDefaults_Partial() throws Exception {
    //Insert Feature
    Feature f1 = XyzSerializable.deserialize("""
        { "type":"Feature",
          "id":"id1",
          "geometry":{"type":"Point","coordinates":[8.0,50.0]},
          "properties":{"firstName":"Alice","age":35}
        }
        """, Feature.class);

    writeFeature(f1, DEFAULT_AUTHOR, null, null,
        null, null, false, SpaceContext.EXTENSION, false);

    Feature f2 = XyzSerializable.deserialize("""
        { 
          "type":"Feature",
          "id":"id1",           
          "geometry":{"type":"Point","coordinates":[50.0,50.0]},   
          "properties": {"new":"value"}
         }
        """, Feature.class);
    //second write with partial modifications
    writeFeature(f2, UPDATE_AUTHOR, null, null,
        null, null, true, SpaceContext.EXTENSION, false);

    Feature expected = XyzSerializable.deserialize("""
        { 
          "type":"Feature",            
          "id":"id1",
          "geometry":{"type":"Point","coordinates":[50.0,50.0]},
          "properties":{"firstName":"Alice","age":35, "new" : "value"}
         }
        """, Feature.class);

    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, UPDATE_AUTHOR);
  }
}
