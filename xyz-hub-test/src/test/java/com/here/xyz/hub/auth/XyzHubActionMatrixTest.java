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

package com.here.xyz.hub.auth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.vertx.core.json.Json;
import org.junit.Test;

public class XyzHubActionMatrixTest {

  @Test
  public void testCase0() {
    String rights = "{'readFeatures': [{'owner': 'O1'}, {'owner': 'O2', 'space': 'S2'}], 'manageSpaces': [{}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'owner': 'O1', 'space': 'S1'}, {'owner': 'O2', 'space': 'S2'}], 'manageSpaces': [{'owner': 'O1', 'space': 'S1'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase1() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'color': 'red', 'size': 'big'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue', 'size': 'small'}, {'color': 'red', 'size': 'big'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase2() {
    String rights = "{'readFeatures': [{'packages': 'HERE'}, {'owner': 'lucas'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'packages': 'HERE'}, {'owner': 'lucas', 'space': 'x'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase3() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'color': 'red', 'size': 'big'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue', 'size': 'small'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase4() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'color': 'red', 'size': 'big'}]}".replace('\'', '"');
    String filter = "{'readFeatures': []}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase5() {
    String rights = "{'readFeatures': [{}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'red', 'size': 'small'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase6() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'tags': ['restaurant', 'open24hrs']}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase7() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'tags': 'restaurant'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue'}, {'tags': ['restaurant', 'open24hrs']}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase8() {
    String rights = "{'readFeatures': [{'owner': 'abc'}, {'packages': 'HERE'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'packages': ['HERE']}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase9() {
    String rights = "{'readFeatures': [{'owner': 'abc'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'owner': 'abc'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertTrue(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase10() {
    String rights = "{'readFeatures': [{'owner': 'abc'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'owner': 'abc', 'space': '123'}, {'owner': 'abc', 'space': '456'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase11() {
    String rights = "{'readFeatures': [{}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'owner': 'abczz'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase12() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'tags': ['restaurant']}, {'tags': ['open24hrs']}, {'tags': ['xxl']}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue'}, {'tags': 'xxl'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase13() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'tags': ['restaurant, open24hrs']}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue'}, {'tags': 'restaurant'}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertFalse(rightsMatrix.matches(filterMatrix));
  }

  @Test
  public void testCase14() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'tags': 'restaurant'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue'}, {'tags': ['restaurant', 'open24hrs']}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
  }

  @Test
  public void testCase15() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'tags': ['restaurant']}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue'}, {'tags': ['restaurant', 'open24hrs']}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
    assertFalse(filterMatrix.matches(rightsMatrix));
  }

  @Test
  public void testCase16() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'tags': 'mapcreator'}, {'tags': 'OSM'}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue'}, {'tags': ['mapcreator', 'delta']}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
  }

  @Test
  public void testCase17() {
    String rights = "{'readFeatures': [{'color': 'blue'}, {'tags': ['mapcreator', 'delta']}, {'tags': ['mapcreator', 'violations']}]}".replace('\'', '"');
    String filter = "{'readFeatures': [{'color': 'blue'}, {'tags': ['mapcreator', 'violations']}]}".replace('\'', '"');

    ActionMatrix rightsMatrix = Json.decodeValue(rights, ActionMatrix.class);
    ActionMatrix filterMatrix = Json.decodeValue(filter, ActionMatrix.class);
    assertTrue(rightsMatrix.matches(filterMatrix));
  }
}
