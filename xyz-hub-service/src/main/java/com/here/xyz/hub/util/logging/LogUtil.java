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
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_INFO;
import static io.vertx.core.http.HttpHeaders.ACCEPT;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static io.vertx.core.http.HttpHeaders.ORIGIN;
import static io.vertx.core.http.HttpHeaders.REFERER;
import static io.vertx.core.http.HttpHeaders.USER_AGENT;
import static io.vertx.core.http.HttpMethod.PATCH;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;

import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.rest.Api;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.Http2ServerResponseImpl;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

public class LogUtil {

  private static final Logger logger = LogManager.getLogger();
  private static final Level STREAM_LEVEL = Level.forName("STREAM", 50);
  private static final Marker ACCESS_LOG_MARKER = MarkerManager.getMarker("ACCESS");

  private static List<String> skipLoggingHeaders = Collections.singletonList(X_FORWARDED_FOR);

  public static String responseToLogEntry(RoutingContext context) {
    HttpServerResponse response = context.response();
    StringBuilder buf = new StringBuilder();
    String httpProto = response instanceof Http2ServerResponseImpl ? "HTTP/2" : "HTTP";
    buf.append(httpProto + " Response: \n");
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
    accessLog.respInfo.streamInfo = context.response().headers().get(STREAM_INFO);

    final JWTPayload tokenPayload = Api.Context.getJWT(context);
    if (tokenPayload != null) {
      accessLog.clientInfo.userId = tokenPayload.aid;
      accessLog.clientInfo.appId = tokenPayload.cid;
      accessLog.classified = new String[]{tokenPayload.tid, tokenPayload.jwt};
    }

    return accessLog;
  }

  public static void writeAccessLog(RoutingContext context) {
    final AccessLog accessLog = Api.Context.getAccessLog(context);
    final Marker marker = Api.Context.getMarker(context);

    accessLog.streamId = marker.getName();
    logger.log(STREAM_LEVEL, ACCESS_LOG_MARKER, accessLog.serialize());
  }
}
