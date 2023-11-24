/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.hub;

import static com.here.naksha.lib.common.TestFileLoader.parseJsonFileOrFail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.hub.mock.NakshaHubMock;
import org.junit.jupiter.api.Test;

public class NakshaHubFactoryTest {

  @Test
  public void testNakshaHubMockInstantiation() throws Exception {
    final NakshaHubConfig cfg = parseJsonFileOrFail("mock_config.json", NakshaHubConfig.class);
    final INaksha hub = NakshaHubFactory.getInstance(null, null, cfg, null);
    assertFalse((hub instanceof NakshaHub), "NakshaHub instance was not expected!");
    assertTrue((hub instanceof NakshaHubMock), "Not a NakshaHubMock instance!");
  }
}
