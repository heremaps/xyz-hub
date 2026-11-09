/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Public;
import com.here.xyz.XyzSerializable.Static;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class JobClientInfoTest {

  @Test
  void shouldKeepUnknownPropertiesAsClientProvidedData() throws JsonProcessingException {
    JobClientInfo clientInfo = XyzSerializable.deserialize("{\"team\":\"teamA\",\"retries\":3}", JobClientInfo.class);
    assertEquals(Map.of("team", "teamA", "retries", 3), clientInfo.getClientProvidedData());
  }

  @Test
  void shouldSerializeEmptyClientInfoAsEmptyObject() {
    assertEquals("{}", XyzSerializable.serialize(new JobClientInfo(), Static.class));
    assertEquals("{}", XyzSerializable.serialize(new JobClientInfo(), Public.class));
  }
}
