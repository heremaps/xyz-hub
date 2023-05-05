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

package com.here.xyz.models.geojson;

import static com.here.xyz.models.geojson.WebMercatorTile.forQuadkey;
import static com.here.xyz.models.geojson.WebMercatorTile.forWeb;
import static com.here.xyz.models.geojson.WebMercatorTile.x;
import static com.here.xyz.models.geojson.WebMercatorTile.xy;
import static com.here.xyz.models.geojson.WebMercatorTile.y;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class WebMercatorTileTest {

  @Test
  public void test_pixel() {
    long pixel = xy(0, 1);
    assertEquals(0, x(pixel));
    assertEquals(1, y(pixel));

    pixel = xy(Integer.MIN_VALUE, Integer.MIN_VALUE);
    assertEquals(Integer.MIN_VALUE, x(pixel));
    assertEquals(Integer.MIN_VALUE, y(pixel));

    pixel = xy(Integer.MAX_VALUE, Integer.MAX_VALUE);
    assertEquals(Integer.MAX_VALUE, x(pixel));
    assertEquals(Integer.MAX_VALUE, y(pixel));
  }

  @Test
  public void positiveExamples() {
    forQuadkey("012301230123");
    forQuadkey("10000");
    forQuadkey("0");
    forQuadkey("123");
    forWeb("0_0_0");
    forWeb("1_1_1");
    forWeb("2_3_1");
    forWeb("5_27_10");
    forWeb("10_800_800");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier1() {
    forQuadkey("052301230123");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier2() {
    forQuadkey("1A000");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier3() {
    forQuadkey("-9");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier4() {
    forQuadkey("1234");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier5() {
    forQuadkey("052301230123");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier6() {
    forWeb("0_0_1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier7() {
    forWeb("0_0_1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier8() {
    forWeb("2_4_4");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier9() {
    forWeb("5_10");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidQuadIdentifier10() {
    forWeb("\"10_-1_10");
  }

  @Test
  public void toQuadkey() {
    String quadkey = "120203302133";
    WebMercatorTile qp = forQuadkey(quadkey);
    assertEquals(quadkey, qp.asQuadkey());
  }
}
