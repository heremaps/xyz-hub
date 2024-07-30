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

import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.test.featurewriter.SQLITWriteFeaturesBase;
import org.junit.Test;

public class SQLITWriteFeaturesWithoutHistoryMergeSzenarios extends SQLITWriteFeaturesBase {

  //********************** Feature exists + BaseVersion Conflict + Merge Conflict *******************************/
  @Test
  public void merge_With_DefaultSettings() throws Exception {
    //Default is ERROR
    Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":35}}
            """, Feature.class);

    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"@ns:com:here:xyz":{"version":0},"lastName":"Wonder","age":32}}
            """, Feature.class);

    //Conflict expected
    performMerge(f1, f2, null, null, SQLErrorCodes.XYZ48);
    checkExistingFeature(f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** OnMergeConflict ERROR *******************************/
  @Test
  public void merge_With_OnMergeConflictERROR() throws Exception {
    //Default is ERROR
    Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":35}}
            """, Feature.class);

    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"@ns:com:here:xyz":{"version":0},"lastName":"Wonder","age":32}}
            """, Feature.class);
    //Conflict expected
    performMerge(f1, f2, null, OnMergeConflict.ERROR, SQLErrorCodes.XYZ48);
    checkExistingFeature(f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** No Conflicts *******************************/
  @Test
  public void merge_WithoutConflict() throws Exception {
    Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"firstName":"Alice"}}
            """, Feature.class);

    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"@ns:com:here:xyz":{"version":0},"lastName":"Wonder"}}
            """, Feature.class);
    //No Conflict expected
    Feature expected =  XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"firstName":"Alice","lastName":"Wonder"}}
               """, Feature.class);

    performMerge(f1, f2, expected, OnMergeConflict.REPLACE, null);
    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  @Test
  public void merge_NestedObject_WithoutConflict() throws Exception {
    Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties": {"nested": {"name":"Alice","gender":"female"}}}
            """, Feature.class);

    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties": { "@ns:com:here:xyz":{"version":0}, "nested": {"lastName":"Wonder","age":35}}}
            """, Feature.class);
    //No Conflict expected
    Feature expected =  XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties": {"nested": {"name": "Alice", "gender":"female", "lastName":"Wonder","age":35}}}
               """, Feature.class);

    performMerge(f1, f2, expected,  OnMergeConflict.REPLACE, null);
    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  @Test
  public void merge_Same_Attributes() throws Exception {
    Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":35}}
            """, Feature.class);

    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"@ns:com:here:xyz":{"version":0},"lastName":"Wonder","age":35}}
            """, Feature.class);
    //No Conflict expected
    Feature expected =  XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","lastName":"Wonder","age":35}}
               """, Feature.class);

    performMerge(f1, f2, expected, OnMergeConflict.REPLACE, null);
    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  //********************** Conflicts with OnMergeConflict=REPLACE*******************************/
  @Test
  public void merge_And_Resolve_Conflict_REPLACE() throws Exception {
    Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":30}}
            """, Feature.class);

    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"@ns:com:here:xyz":{"version":0},"lastName":"Wonder","age":35}}
            """, Feature.class);
    //No Conflict expected
    Feature expected =  XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"lastName":"Wonder","age":35}}
               """, Feature.class);

    performMerge(f1, f2, expected,  OnMergeConflict.REPLACE, null);
    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  @Test
  public void merge_NestedObject_And_Resolve_Conflict_REPLACE() throws Exception {
    Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties": {"nested": {"firstName":"Alice","age":30}}}
            """, Feature.class);

    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties": { "@ns:com:here:xyz":{"version":0}, "nested": {"lastName":"Wonder","age":35}}}
            """, Feature.class);
    //No Conflict expected
    Feature expected =  XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties": {"nested": {"lastName":"Wonder","age":35}}}
               """, Feature.class);

    performMerge(f1, f2, expected,  OnMergeConflict.REPLACE, null);
    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }

  @Test
  public void merge_Array_Resolve_Conflict_REPLACE() throws Exception {
     Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"items": ["apple", "banana"] }
            }
            """, Feature.class);

    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties": { "@ns:com:here:xyz":{"version":0}, "items": ["cherry"]}
            }
            """, Feature.class);
    //No Conflict expected
    Feature expected =  XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties": {"items": ["cherry"]}
            }""", Feature.class);

    performMerge(f1, f2, expected, OnMergeConflict.REPLACE, null);
    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, DEFAULT_AUTHOR);
  }
}
