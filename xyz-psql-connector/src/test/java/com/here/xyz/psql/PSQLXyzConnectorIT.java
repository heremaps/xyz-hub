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

package com.here.xyz.psql;

import static org.junit.Assert.assertEquals;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.config.PSQLConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unused")
public class PSQLXyzConnectorIT extends PSQLAbstractIT {
  @Test
  public void testPSQLConfig() throws Exception {
    Map<String, Object> connectorParams = new HashMap<>();
    Map<String, Object> parametersToEncrypt = new HashMap<>();
    parametersToEncrypt.put(DatabaseSettings.PSQL_HOST, "example.com");
    parametersToEncrypt.put(DatabaseSettings.PSQL_PORT, "1234");
    parametersToEncrypt.put(DatabaseSettings.PSQL_PASSWORD, "1234password");
    connectorParams.put("ecps", PSQLConfig.encryptECPS(new ObjectMapper().writeValueAsString(parametersToEncrypt), "testing"));

    HealthCheckEvent event = new HealthCheckEvent()
        .withConnectorParams(connectorParams);

    PSQLConfig config = new PSQLConfig(event, GS_CONTEXT);
    assertEquals(config.getDatabaseSettings().getHost(), "example.com");
    assertEquals(config.getDatabaseSettings().getPort(), 1234);
    assertEquals(config.getDatabaseSettings().getPassword(), "1234password");
  }
}
