/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.task;

import static org.junit.Assert.*;

import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.task.ModifyOp.IfExists;
import com.here.xyz.hub.task.ModifyOp.IfNotExists;
import com.here.xyz.hub.task.ModifyOp.ModifyOpError;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

public class ModifyFeatureOpTest {

  @Test
  public void patch() {
  }

  @Test
  public void merge() throws IOException, ModifyOpError {
    try (
        final InputStream is1 = ModifyFeatureOpTest.class.getResourceAsStream("/xyz/hub/task/FeatureSample01.json")
    ) {
      final Feature baseState = XyzSerializable.deserialize(is1);
      final Feature headState = baseState.copy();
      final Feature input = baseState.copy();

      long now = System.currentTimeMillis();
      headState.getProperties().put("newProperty", 1);
      headState.getProperties().getXyzNamespace().setUuid("new-uuid");
      headState.getProperties().getXyzNamespace().setPuuid(baseState.getProperties().getXyzNamespace().getUuid());
      headState.getProperties().getXyzNamespace().setUpdatedAt(now);

      input.getProperties().put("name", "changed");
      input.getProperties().getXyzNamespace().setCreatedAt(123);
      input.getProperties().getXyzNamespace().getTags().add("tag2");

      ModifyFeatureOp op = new ModifyFeatureOp(null, IfNotExists.CREATE, IfExists.MERGE, true);
      Feature res = op.merge(headState, baseState, input);

      assertNotEquals(res.getProperties().getXyzNamespace().getCreatedAt(), 123);
      assertTrue(res.getProperties().getXyzNamespace().getTags().contains("tag1"));
      assertTrue(res.getProperties().getXyzNamespace().getTags().contains("tag2"));
      assertEquals(res.getProperties().get("name"), "changed");
    } catch (IOException | ModifyOpError e) {
      e.printStackTrace();
    }
  }

  @Test
  public void replace() {
    try (
        final InputStream is1 = ModifyFeatureOpTest.class.getResourceAsStream("/xyz/hub/task/FeatureSample01.json")
    ) {
      final Feature baseState = XyzSerializable.deserialize(is1);
      final Feature input = baseState.copy();
      input.getProperties().put("name", "changed");
      input.getProperties().getXyzNamespace().setCreatedAt(123);
      input.getProperties().getXyzNamespace().getTags().add("tag2");

      ModifyFeatureOp op = new ModifyFeatureOp(null, IfNotExists.CREATE, IfExists.MERGE, true);
      Feature res = op.replace(baseState, input);

      assertNotEquals(res.getProperties().getXyzNamespace().getCreatedAt(), 123);
      assertTrue(res.getProperties().getXyzNamespace().getTags().contains("tag1"));
      assertTrue(res.getProperties().getXyzNamespace().getTags().contains("tag2"));
      assertEquals(res.getProperties().get("name"), "changed");
    } catch (IOException | ModifyOpError e) {
      e.printStackTrace();
    }
  }
}