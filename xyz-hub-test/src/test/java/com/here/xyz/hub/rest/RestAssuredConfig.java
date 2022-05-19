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

package com.here.xyz.hub.rest;


import com.here.xyz.util.EnvName;
import com.here.xyz.util.JsonConfigFile;
import com.here.xyz.util.JsonName;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RestAssuredConfig extends JsonConfigFile<RestAssuredConfig> {

  private static final Logger logger = LogManager.getLogger();

  // We annotate the well-known environment variable names and the JSON names from the XYZ-hub service config.
  // Additionally, we define special environment names.

  @EnvName("HOST_NAME") // config.json
  @JsonName("HOST_NAME") // config.json
  public String hubHost;
  @EnvName("HTTP_HOST")
  @EnvName("PSQL_HTTP_CONNECTOR_HOST") // config.json
  @JsonName("PSQL_HTTP_CONNECTOR_HOST") // config.json
  public String connectorHost = "localhost";
  @EnvName("HTTP_PORT") // config.json and connector-config.json (both must run at different ports, but share an env-var!)
  @JsonName("HTTP_PORT") // config.json
  public int hubPort = 8080;
  /**
   * When the port is shared with the XYZ-Hub, then the embedded connector is tested. If that is not wished, you can use the environment
   * variable HTTP_CONNECTOR_PORT to override the connector port to the one at which the standalone connector is running.
   */
  @EnvName("PSQL_HTTP_CONNECTOR_PORT") // config.json
  @JsonName("PSQL_HTTP_CONNECTOR_PORT") // config.json
  public int connectorPort = 9090;
  @EnvName("ECPS_PHRASE") // connector-config.json
  @JsonName("ECPS_PHRASE") // connector-config.json
  @EnvName("DEFAULT_ECPS_PHRASE") // config.json
  @JsonName("DEFAULT_ECPS_PHRASE") // config.json
  public String ecpsPhrase = "local";
  @EnvName("STORAGE_DB_URL")
  public String STORAGE_DB_URL = "jdbc:postgresql://localhost/postgres";
  @EnvName("STORAGE_DB_USER")
  @EnvName("PSQL_USER")
  public String STORAGE_DB_USER = "postgres";
  @EnvName("STORAGE_DB_PASSWORD")
  @EnvName("PSQL_PASSWORD")
  public String STORAGE_DB_PASSWORD = "password";
  @EnvName("PSQL_DB")
  public String STORAGE_DB = "postgres";
  @EnvName("PSQL_SCHEMA")
  public String STORAGE_SCHEMA = "public";
  @EnvName("PSQL_HOST")
  public String STORAGE_HOST = "localhost";
  @EnvName("PSQL_PORT")
  public int STORAGE_PORT = 5432;
  /**
   * The unencrypted ECPS JSON.
   */
  public String ecpsJson;
  public String fullHubUri;
  public String fullHttpConnectorUri;

  private RestAssuredConfig() {
    try {
      load();
    } catch (IOException e) {
      throw new Error(e);
    }
    try {
      final Pattern uriPattern = Pattern.compile("(jdbc):(postgresql)://([a-zA-Z0-9-.]+)(:[0-9]{1,5})?/([a-zA-Z0-9-.]+)");
      final Matcher matcher = uriPattern.matcher(STORAGE_DB_URL);
      if (matcher.find()) {
        // psql = 1
        // postgresql = 2
        STORAGE_HOST = matcher.group(3);
        final String port = matcher.group(4);
        if (port != null) {
          STORAGE_PORT = Integer.parseInt(port, 10);
        }
        STORAGE_DB = matcher.group(5);
      }
    } catch (Exception ignore) {
    }
    fullHubUri = "http://" + hubHost + ":" + hubPort + "/hub";
    fullHttpConnectorUri = "http://" + connectorHost + ":" + connectorPort + "/psql";
    ecpsJson = "{"
        + "\"PSQL_HOST\":\"" + STORAGE_HOST + "\","
        + "\"PSQL_PORT\":\"" + STORAGE_PORT + "\","
        + "\"PSQL_DB\":\"" + STORAGE_DB + "\","
        + "\"PSQL_USER\":\"" + STORAGE_DB_USER + "\","
        + "\"PSQL_PASSWORD\":\"" + STORAGE_DB_PASSWORD + "\","
        + "\"PSQL_SCHEMA\":\"" + STORAGE_SCHEMA + "\""
        + "}";
  }

  private static RestAssuredConfig config = null;

  public static RestAssuredConfig config() {
    if (config == null) {
      config = new RestAssuredConfig();
    }
    return config;
  }

  @Nullable
  @Override
  protected String defaultFile() {
    return "config.json";
  }

  protected void info(String message) {
    logger.info(message);
  }

  protected void error(String message, Throwable t) {
    logger.error(message, t);
  }
}
