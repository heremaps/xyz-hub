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

import static com.here.xyz.psql.DatabaseHandler.ECPS_PHRASE;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.runtime.FunctionRuntime;
import java.util.HashMap;
import java.util.Map;
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
    connectorParams.put("ecps", ECPSTool.encrypt("testing", new ObjectMapper().writeValueAsString(parametersToEncrypt)));

    HealthCheckEvent event = new HealthCheckEvent()
        .withConnectorParams(connectorParams);

    String phrase = FunctionRuntime.getInstance().getEnvironmentVariable(ECPS_PHRASE);
    DatabaseSettings dbSettings = new DatabaseSettings(getClass().getSimpleName(),
        ECPSTool.decryptToMap(phrase, XyzSerializable.fromMap(connectorParams, ConnectorParameters.class).getEcps()));

    ConnectorParameters connectorParameters = XyzSerializable.fromMap(event.getConnectorParams(), ConnectorParameters.class);
    assertEquals(dbSettings.getHost(), "example.com");
    assertEquals(dbSettings.getPort(), 1234);
    assertEquals(dbSettings.getPassword(), "1234password");
  }
}
