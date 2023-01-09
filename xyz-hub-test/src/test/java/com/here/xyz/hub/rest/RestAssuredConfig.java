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

public class RestAssuredConfig {
  public String baseURI;
  public int hubPort;
  public String fullHubUri;
  public String fullHttpConnectorUri;

  private RestAssuredConfig() {
  }

  private static RestAssuredConfig config = null;

  public static RestAssuredConfig config() {
    if (config == null) {
      config = localConfig();
    }
    return config;
  }

  private static RestAssuredConfig localConfig() {
    RestAssuredConfig config = new RestAssuredConfig();
    String envPort = System.getenv("HTTP_PORT");
    String host = System.getenv().containsKey("HTTP_HOST") ? System.getenv("HTTP_HOST") : "localhost";
    String service = System.getenv().containsKey("HTTP_SERVICE") ? System.getenv("HTTP_SERVICE") : "hub";
    config.baseURI = "http://"+host+"/" + service;
    config.hubPort = 8080;
    config.fullHubUri = "http://"+host+":"+config.hubPort +"/" + service;
    config.fullHttpConnectorUri = "http://"+host+":9090/psql";

    try {
      config.hubPort = Integer.parseInt(envPort);
    }
    catch (NumberFormatException ignore) {}
    return config;
  }
}
