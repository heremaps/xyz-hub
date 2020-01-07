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

package com.here.xyz.hub.util.logging;

import static com.google.common.net.HttpHeaders.X_FORWARDED_FOR;
import static io.vertx.core.http.HttpHeaders.ACCEPT;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static io.vertx.core.http.HttpHeaders.ORIGIN;
import static io.vertx.core.http.HttpHeaders.REFERER;
import static io.vertx.core.http.HttpHeaders.USER_AGENT;
import static io.vertx.core.http.HttpMethod.PATCH;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.rest.Api;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class LogUtil {

  private static final Logger logger = LogManager.getLogger();

  private static List<String> skipLoggingHeaders = Collections.singletonList(X_FORWARDED_FOR);

  public static void logRequest(RoutingContext context) {
    final HttpServerRequest request = context.request();
    StringBuilder buf = new StringBuilder();
    buf.append("HTTP Request: \n");
    buf.append(request.method());
    buf.append(' ');
    buf.append(request.uri());
    buf.append('\n');

    appendHeaders(request.headers(), buf);

    buf.append('\n');
    buf.append(context.getBodyAsString());
    logger.info(Api.Context.getMarker(context), "{}", buf.toString().trim());
  }

  public static String responseToLogEntry(RoutingContext context) {
    HttpServerResponse response = context.response();
    StringBuilder buf = new StringBuilder();
    buf.append("HTTP Response: \n");
    buf.append(response.getStatusCode());
    buf.append(' ');
    buf.append(response.getStatusMessage());
    buf.append('\n');

    appendHeaders(response.headers(), buf);
    return buf.toString().trim();
  }

  private static void appendHeaders(MultiMap headers, StringBuilder buf) {
    for (Map.Entry<String, String> header : headers) {
      if (!skipLoggingHeaders.contains(header.getKey())) {
        buf.append(header.getKey());
        buf.append(" : ");
        buf.append(header.getValue());
        buf.append('\n');
      }
    }
  }

  /**
   * Add the basic request information into the marker.
   *
   * @param context the context of the marker to become modified.
   */
  public static void addRequestInfo(RoutingContext context) {
    AccessLog accessLog = Api.Context.getAccessLog(context);
    HttpMethod method = context.request().method();
    accessLog.reqInfo.method = context.request().method().name();
    accessLog.reqInfo.uri = context.request().uri();
    accessLog.reqInfo.referer = context.request().getHeader(REFERER);
    accessLog.reqInfo.origin = context.request().getHeader(ORIGIN);
    if (POST.equals(method) || PUT.equals(method) || PATCH.equals(method)) {
      accessLog.reqInfo.size = context.getBody().length();
    }
    accessLog.clientInfo.userAgent = context.request().getHeader(USER_AGENT);
    accessLog.reqInfo.contentType = context.request().getHeader(CONTENT_TYPE);
    accessLog.reqInfo.accept = context.request().getHeader(ACCEPT);
  }

  /**
   * Add the response information into the marker. As the authentication is done after the request has been received, this method will as
   * well add the client-id to the request information. So, even while the client-id is part of the request information, for technical
   * reasons it's added together with the response information, because the JWT token is processed after the {@link
   * #addRequestInfo(RoutingContext)} was invoked and therefore this method does not have the necessary information.
   *
   * @param context the context of the marker to become modified.
   */
  public static AccessLog addResponseInfo(RoutingContext context) {
    AccessLog accessLog = Api.Context.getAccessLog(context);
    accessLog.respInfo.statusCode = context.response().getStatusCode();
    accessLog.respInfo.statusMsg = context.response().getStatusMessage();
    accessLog.respInfo.size = context.response().bytesWritten();

    final JWTPayload jwt = Api.Context.getJWT(context);
    if (jwt != null) {
      accessLog.clientInfo.userId = jwt.aid;
      accessLog.clientInfo.appId = jwt.cid;
    }

    return accessLog;
  }

  public static void writeAccessLog(RoutingContext context) {
    final AccessLog accessLog = Api.Context.getAccessLog(context);
    final Marker marker = Api.Context.getMarker(context);
    accessLog.streamId = marker.getName();
    final String formattedLog = XyzSerializable.serialize(accessLog, new TypeReference<AccessLog>() {
    });
    logger.warn(marker, formattedLog);
  }
}
