/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.util.service.logging;

import static com.google.common.net.HttpHeaders.X_FORWARDED_FOR;
import static com.here.xyz.util.Random.randomAlphaNumeric;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.STREAM_ID;
import static com.here.xyz.util.service.BaseHttpServerVerticle.STREAM_INFO_CTX_KEY;
import static com.here.xyz.util.service.BaseHttpServerVerticle.getAuthor;
import static com.here.xyz.util.service.BaseHttpServerVerticle.getJWT;
import static io.vertx.core.http.HttpHeaders.ACCEPT;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static io.vertx.core.http.HttpHeaders.ORIGIN;
import static io.vertx.core.http.HttpHeaders.REFERER;
import static io.vertx.core.http.HttpHeaders.USER_AGENT;
import static io.vertx.core.http.HttpMethod.PATCH;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;

import com.google.common.base.Strings;
import com.here.xyz.models.hub.jwt.JWTPayload;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.Http2ServerResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

public class LogUtil {
  private static final Logger logger = LogManager.getLogger();
  private static final Level STREAM_LEVEL = Level.forName("STREAM", 50);
  private static final Marker ACCESS_LOG_MARKER = MarkerManager.getMarker("ACCESS");
  private static final String REALM = "rlm";
  private static final String MARKER = "marker";
  private static final String ACCESS_LOG = "accessLog";

  private static List<String> skipLoggingHeaders = Collections.singletonList(X_FORWARDED_FOR);

  private static final String IPV4_REGEX = "^(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])(\\.(?!$)|$)){4}$";
  private static final String IPV6_STD_REGEX = "^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$";
  private static final String IPV6_HEX_COMPRESSED_REGEX = "^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$";

  private static final Pattern IPV4_PATTERN = Pattern.compile(IPV4_REGEX);
  private static final Pattern IPV6_STD_PATTERN = Pattern.compile(IPV6_STD_REGEX);
  private static final Pattern IPV6_HEX_COMPRESSED_PATTERN = Pattern.compile(IPV6_HEX_COMPRESSED_REGEX);

  private static String getIp(RoutingContext context) {
    String ips = context.request().getHeader(X_FORWARDED_FOR);
    if (!Strings.isNullOrEmpty(ips)) {
      String ip = ips.split(", ")[0];

      if (IPV4_PATTERN.matcher(ip).matches() ||
          IPV6_STD_PATTERN.matcher(ip).matches() ||
          IPV6_HEX_COMPRESSED_PATTERN.matcher(ip).matches()) {
        return ip;
      }
    }

    return context.request().connection().remoteAddress().host();
  }

  public static String responseToLogEntry(RoutingContext context) {
    HttpServerResponse response = context.response();
    StringBuilder buf = new StringBuilder();
    String httpProto = response instanceof Http2ServerResponse ? "HTTP/2" : "HTTP";
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
    AccessLog accessLog = getAccessLog(context);
    HttpMethod method = context.request().method();
    accessLog.reqInfo.method = context.request().method().name();
    accessLog.reqInfo.uri = context.request().uri();
    accessLog.reqInfo.referer = context.request().getHeader(REFERER);
    accessLog.reqInfo.origin = context.request().getHeader(ORIGIN);

    if (POST.equals(method) || PUT.equals(method) || PATCH.equals(method))
      accessLog.reqInfo.size = context.getBody() == null ? 0 : context.getBody().length();

    accessLog.clientInfo.ip = getIp(context);
    accessLog.clientInfo.remoteAddress = context.request().connection().remoteAddress().toString();
    accessLog.clientInfo.userAgent = context.request().getHeader(USER_AGENT);
    accessLog.clientInfo.realm = context.request().getHeader(REALM);
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
    AccessLog accessLog = getAccessLog(context);
    accessLog.respInfo.statusCode = context.response().getStatusCode();
    accessLog.respInfo.statusMsg = context.response().getStatusMessage();
    accessLog.respInfo.size = context.response().bytesWritten();
    accessLog.respInfo.contentType = context.response().headers().get(CONTENT_TYPE);

    accessLog.streamInfo = context.get(STREAM_INFO_CTX_KEY);

    final JWTPayload tokenPayload = getJWT(context);
    if (tokenPayload != null) {
      accessLog.clientInfo.userId = tokenPayload.aid;
      accessLog.clientInfo.appId = tokenPayload.cid != null ? tokenPayload.cid : getAuthor(context);
      accessLog.classified = new String[]{tokenPayload.tid, tokenPayload.jwt};
    }
    else {
      final String ACCESS_TOKEN = "access_token";
      String token = context.get(ACCESS_TOKEN); //Could be a token ID or an actual JWT, both have to be classified
      if (token != null && token != "")
        accessLog.classified = new String[]{context.get(ACCESS_TOKEN)};
    }

    return accessLog;
  }

  public static void writeAccessLog(RoutingContext context) {
    final AccessLog accessLog = getAccessLog(context);
    final Marker marker = getMarker(context);

    accessLog.streamId = marker.getName();
    logger.log(STREAM_LEVEL, ACCESS_LOG_MARKER, accessLog.serialize());
  }

  /**
   * Returns the log marker for the request.
   *
   * @return the marker or null, if no marker was found.
   */
  public static Marker getMarker(RoutingContext context) {
    if (context == null)
      return null;

    Marker marker = context.get(MARKER);
    if (marker == null) {
      String sid = context.request().getHeader(STREAM_ID);
      marker = new Log4jMarker(sid != null ? sid : randomAlphaNumeric(10));
      context.put(MARKER, marker);
    }
    return marker;
  }

  /**
   * Returns the access log object for this request.
   *
   * @param context the routing context.
   * @return the access log object
   */
  public static AccessLog getAccessLog(RoutingContext context) {
    if (context == null)
      return null;

    AccessLog accessLog = context.get(ACCESS_LOG);
    if (accessLog == null) {
      accessLog = new AccessLog();
      context.put(ACCESS_LOG, accessLog);
    }
    return accessLog;
  }
}
