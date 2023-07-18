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
package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.TIMEOUT;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNAVAILABLE;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;
import static com.here.xyz.hub.util.health.schema.Status.Result.WARNING;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;
import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Map;

public class ServiceHealthCheck extends ExecutableCheck {

  protected URL endpoint;
  private ObjectMapper jsonMapper = new ObjectMapper();

  public ServiceHealthCheck(URL endpoint) {
    setTarget(Target.REMOTE);
    setRole(Role.SERVICE);
    this.endpoint = endpoint;
  }

  public ServiceHealthCheck(URL endpoint, int timeout) {
    this(endpoint);
    this.timeout = timeout;
  }

  public ServiceHealthCheck(URL endpoint, String name) {
    this(endpoint);
    setName(name);
  }

  public ServiceHealthCheck(String endpoint, String name) throws MalformedURLException {
    this(new URL(endpoint), name);
  }

  public ServiceHealthCheck(String endpoint, String name, int timeout) throws MalformedURLException {
    this(new URL(endpoint), timeout);
    setName(name);
  }

  private Response parseIfPossible(String serviceResponse) {
    Response r;
    try {
      r = jsonMapper.readValue(serviceResponse, Response.class);
      if (r.getStatus() == null) {
        // Try parsing it as arbitrary JSON object
        r = new Response();
        r.setAdditionalProperty("json", jsonMapper.readValue(serviceResponse, Map.class));
      }
      return r;
    } catch (IOException e) {
      // It's no JSON
      r = new Response();
      r.setMessage(
          serviceResponse.length() > 512 ? serviceResponse.substring(0, 512 - 4) + " ..." : serviceResponse);
      return r;
    }
  }

  private Status doRequest(Status status, URL url) {
    try {
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setConnectTimeout(timeout);
      conn.setReadTimeout(timeout);
      conn.connect();

      Response response;
      try (Reader r = new InputStreamReader(conn.getInputStream())) {
        response = parseIfPossible(CharStreams.toString(r));
      }
      setResponse(response);

      int responseCode = conn.getResponseCode();
      response.setAdditionalProperty("statusCode", responseCode);
      if (responseCode >= 200 && responseCode <= 299) {
        if (status.getResult() != WARNING) status.setResult(OK);
      } else if (responseCode == HttpURLConnection.HTTP_NOT_MODIFIED) {
        status.setResult(UNKNOWN);
      } else if (responseCode >= 300 && responseCode <= 399) {
        String previousWarnMessage = "";
        if (status.getResult() == WARNING)
          previousWarnMessage =
              (String) status.getAdditionalProperties().get("warnMessage") + "\n";

        status.setResult(WARNING);
        String warnMessage = previousWarnMessage + "Received redirection.";
        URL targetLocation;
        try {
          targetLocation = getTargetLocation(conn);
        } catch (MalformedURLException e) {
          targetLocation = null;
        }
        ;

        if (targetLocation == null) {
          warnMessage += " No (valid) relocation target was given in the response headers.";
          // If we couldn't resolve a relocation target the target service isn't available for us
          status.setResult(UNAVAILABLE);
        } else {
          if (responseCode == HttpURLConnection.HTTP_MULT_CHOICE
              || responseCode == HttpURLConnection.HTTP_MOVED_PERM
              || responseCode == HttpURLConnection.HTTP_SEE_OTHER
              || responseCode == HttpURLConnection.HTTP_USE_PROXY
              || responseCode == 306
              || responseCode == 308) {
            // For all permanent redirects suggest the user to switch to the redirection target
            warnMessage += " Please consider changing this check to use target location directly.";
          }
          warnMessage += " Target location: " + targetLocation.toString();
          doRequest(status, targetLocation);
        }
        status.setAdditionalProperty("warnMessage", warnMessage);
      } else if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
        status.setResult(UNAVAILABLE);
      } else if (responseCode >= 400) {
        status.setResult(ERROR);
      }

      if (response.getStatus() != null) {
        status.setResult(
            getWorseResult(status.getResult(), response.getStatus().getResult()));
      }
    } catch (SocketTimeoutException ste) {
      status.setResult(TIMEOUT);
    } catch (IOException e) {
      status.setResult(ERROR);
      status.setAdditionalProperty("errorMessage", e.getMessage());
    }
    return status;
  }

  @JsonIgnore
  private URL getTargetLocation(HttpURLConnection conn) throws MalformedURLException {
    String targetLocation = conn.getHeaderField("Location");
    if (targetLocation == null) targetLocation = conn.getHeaderField("location");
    return new URL(targetLocation);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Status execute() {
    if (endpoint == null) {
      setResponse(new Response().withMessage("Can't do the service health check. Endpoint URL is missing."));
      return new Status().withResult(UNKNOWN);
    }
    return doRequest(new Status(), endpoint);
  }
}
