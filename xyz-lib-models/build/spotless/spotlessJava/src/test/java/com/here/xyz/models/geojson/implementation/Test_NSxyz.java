/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.models.geojson.implementation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class Test_NSxyz {

  @Test
  public void test_fixNormalizedTags() {
    final List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b,c");
    assertEquals(2, tags.size());
    XyzNamespace.fixNormalizedTags(tags);
    assertEquals(3, tags.size());
    assertEquals("a", tags.get(0));
    assertEquals("c", tags.get(1));
    assertEquals("b", tags.get(2));
  }
}
