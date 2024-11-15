/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.hub;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;

import com.here.xyz.util.service.BaseConfig;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import com.here.xyz.util.service.HttpException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public abstract class AbstractHttpServerVerticle extends BaseHttpServerVerticle {

  private static final Logger logger = LogManager.getLogger();
  private static List<Consumer<RoutingContext>> responseEndObservers = new ArrayList<>();

  /**
   * The max request size handler.
   */
  @Override
  protected Handler<RoutingContext> createMaxRequestSizeHandler() {
    return context -> {
      if(Service.configuration != null ) {
        long limit = BaseConfig.instance.MAX_UNCOMPRESSED_REQUEST_SIZE;

        String errorMessage = "The request payload is bigger than the maximum allowed.";
        String uploadLimit;
        HttpResponseStatus status = REQUEST_ENTITY_TOO_LARGE;

        if (BaseConfig.instance.UPLOAD_LIMIT_HEADER_NAME != null
                && (uploadLimit = context.request().headers().get(Service.configuration.UPLOAD_LIMIT_HEADER_NAME))!= null) {

          try {
             /** Override limit if we are receiving an UPLOAD_LIMIT_HEADER_NAME value */
            limit = Long.parseLong(uploadLimit);

            /** Add limit to streamInfo response header */
            XYZHubRESTVerticle.addStreamInfo(context, "MaxReqSize", limit);
          } catch (NumberFormatException e) {
            sendErrorResponse(context, new HttpException(BAD_REQUEST, "Value of header: " + Service.configuration.UPLOAD_LIMIT_HEADER_NAME + " has to be a number."));
            return;
          }

          /** Override http response code if its configured */
          if(Service.configuration.UPLOAD_LIMIT_REACHED_HTTP_CODE > 0)
            status = HttpResponseStatus.valueOf(Service.configuration.UPLOAD_LIMIT_REACHED_HTTP_CODE);

          /** Override error Message if its configured */
          if(Service.configuration.UPLOAD_LIMIT_REACHED_MESSAGE != null)
            errorMessage = Service.configuration.UPLOAD_LIMIT_REACHED_MESSAGE;
        }

        if (limit > 0) {
          if (context.getBody() != null && context.getBody().length() > limit) {
            sendErrorResponse(context, new HttpException(status, errorMessage));
            return;
          }
        }
      }
      try{ context.next(); }
      catch(IllegalArgumentException e)
      { sendErrorResponse(context, new HttpException(BAD_REQUEST,"", e));
        return;
      }
    };
  }

  public static void registerResponseEndObservers(Consumer<RoutingContext> consumer) {
    responseEndObservers.add(consumer);
  }

  @Override
  protected void onResponseEnd(RoutingContext context) {
    super.onResponseEnd(context);
    responseEndObservers.forEach(c -> {
      try {
        c.accept(context);
      } catch (Exception e) {
        logger.error("Error during execution of response end observer", e);
      }
    });
  }
}
