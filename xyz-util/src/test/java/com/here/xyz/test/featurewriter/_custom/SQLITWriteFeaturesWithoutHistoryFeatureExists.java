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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import org.junit.Test;

public class SQLITWriteFeaturesWithoutHistoryFeatureExists extends SQLITWriteFeaturesBase {
  private Feature default_f1;

  public SQLITWriteFeaturesWithoutHistoryFeatureExists() throws JsonProcessingException {
      super(false);
      default_f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice"}
            }
            """, Feature.class);
  }

  //********************** Feature exists (OnVersionConflict deactivated) *******************************/
  @Test
  public void writeToExistingFeature_OnExistsDELETE() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"will":"fail"}
            }
            """, Feature.class);
    writeFeature(f2, UPDATE_AUTHOR, OnExists.DELETE,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(DEFAULT_FEATURE_ID);
  }

  @Test
  public void writeToExistingFeature_OnExistsREPLACE() throws Exception {
    //initial write
    Feature f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice"}
            }
            """, Feature.class);
    writeFeature(f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"age":35, "gender":"female"}
            }
            """, Feature.class);

    writeFeature(f2, UPDATE_AUTHOR, OnExists.REPLACE,null,null,null,
            false, SpaceContext.EXTENSION, false, null);

    Feature expected = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"age":35, "gender":"female"}
            }
            """, Feature.class);

    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, UPDATE_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_OnExistsRETAIN() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"will":"retain"}
            }
            """, Feature.class);

    writeFeature(f2, UPDATE_AUTHOR, OnExists.RETAIN,null,null,null,
            false, SpaceContext.EXTENSION, false, null);
    checkExistingFeature(default_f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_OnExistsERROR() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"will":"error"}
            }
            """, Feature.class);
    writeFeature(f2, UPDATE_AUTHOR, OnExists.ERROR,null,null,null,
            false, SpaceContext.EXTENSION, false, SQLError.FEATURE_EXISTS);

    checkExistingFeature(default_f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** Feature exists (OnVersionConflict.REPLACE + BaseVersion Match) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsDELETE() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);
    //Add matching Baseversion
    default_f1.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1L));

    //Second write with modifications
    writeFeature(default_f1, UPDATE_AUTHOR, OnExists.DELETE,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkNotExistingFeature(default_f1.getId());
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsREPLACE() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications and matching baseversion
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{ "@ns:com:here:xyz":{"version":1}, "age":35, "gender":"female"}
            }
            """, Feature.class);

    writeFeature(f2, UPDATE_AUTHOR, OnExists.REPLACE,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(f2, 2L, Long.MAX_VALUE, Operation.U, UPDATE_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsRETAIN() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications and matching baseversion
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"@ns:com:here:xyz":{"version":1}, "will":"retain"}
            }
            """, Feature.class);
    writeFeature(f2, UPDATE_AUTHOR,OnExists.RETAIN,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(default_f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  @Test
  public void writeToExistingFeature_WithBaseVersion_WithoutConflict_OnExistsERROR() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications and matching baseversion
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"@ns:com:here:xyz":{"version":1}, "will":"error"}
            }
            """, Feature.class);
    writeFeature(f2, UPDATE_AUTHOR, OnExists.ERROR,null, OnVersionConflict.REPLACE,null,
            false, SpaceContext.EXTENSION, false, SQLError.FEATURE_EXISTS);

    checkExistingFeature(default_f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** Feature exists + BaseVersion Conflict (onVersionConflict.ERROR) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictERROR() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications and NOT matching baseversion
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"@ns:com:here:xyz":{"version":0}, "will":"error"}
            }
            """, Feature.class);

    writeFeature(f2, UPDATE_AUTHOR, null,null, OnVersionConflict.ERROR, null,
            false, SpaceContext.EXTENSION, false, SQLError.VERSION_CONFLICT_ERROR);
    checkExistingFeature(default_f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** Feature exists + BaseVersion Conflict (onVersionConflict.RETAIN) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictRETAIN() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications and NOT matching baseversion
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"@ns:com:here:xyz":{"version":0}, "will":"error"}
            }
            """, Feature.class);
    writeFeature(f2, UPDATE_AUTHOR, null,null, OnVersionConflict.RETAIN, null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(default_f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
  }

  //********************** Feature exists + BaseVersion Conflict (OnVersionConflict.REPLACE) *******************************/
  @Test
  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictREPLACE() throws Exception {
    //initial write
    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
            null, null, false, SpaceContext.EXTENSION,false, null);

    //Second write with modifications and NOT matching baseversion
    Feature f2 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "properties":{"@ns:com:here:xyz":{"version":0}, "new":"value"}
            }
            """, Feature.class);
    writeFeature(f2, UPDATE_AUTHOR,null,null, OnVersionConflict.REPLACE, null,
            false, SpaceContext.EXTENSION, false, null);

    checkExistingFeature(f2, 2L, Long.MAX_VALUE, Operation.U, UPDATE_AUTHOR);
  }

  //********************** Feature exists + BaseVersion Conflict + no merge conflict (OnVersionConflict.MERGE) *******************************/
//  @Test
//  public void writeToExistingFeature_WithBaseVersion_Conflict_OnVersionConflictMERGE() throws Exception {
//    //initial write
//    writeFeature(default_f1, DEFAULT_AUTHOR, null , null,
//            null, null, false, SpaceContext.EXTENSION,false, null);
//
//    //Second write with modifications and NOT matching baseversion
//    Feature f2 = XyzSerializable.deserialize("""
//            { "type":"Feature",
//              "id":"id1",
//              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
//              "properties":{"@ns:com:here:xyz":{"version":0}, "new":"value"}
//            }
//            """, Feature.class);
//
//    writeFeature(f2, UPDATE_AUTHOR,null,null, OnVersionConflict.MERGE, null,
//            false, SpaceContext.EXTENSION, false, null);
//
//    Feature expected = XyzSerializable.deserialize("""
//            { "type":"Feature",
//              "id":"id1",
//              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
//              "properties":{"@ns:com:here:xyz":{"version":0}, "new":"value", "firstName":"Alice"}
//            }
//            """, Feature.class);
//    checkExistingFeature(expected, 2L, Long.MAX_VALUE, Operation.U, UPDATE_AUTHOR);
//  }
}
