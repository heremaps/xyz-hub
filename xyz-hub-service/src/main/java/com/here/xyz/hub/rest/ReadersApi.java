/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;

public class ReadersApi extends SpaceBasedApi {

  public ReadersApi(RouterBuilder rb) {
    rb.operation("putReader").handler(this::putReader);
    rb.operation("deleteReader").handler(this::deleteReader);
    rb.operation("getReaderVersion").handler(this::getReaderVersion);
    rb.operation("increaseReaderVersion").handler(this::increaseReaderVersion);
  }

  private void putReader(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String reader = Query.getString(context, Query.READER_ID, null);
  }

  private void deleteReader(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String reader = Query.getString(context, Query.READER_ID, null);
  }

  private void getReaderVersion(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String reader = Query.getString(context, Query.READER_ID, null);
  }

  private void increaseReaderVersion(RoutingContext context) {
    final String spaceId = context.pathParam(Path.SPACE_ID);
    final String reader = Query.getString(context, Query.READER_ID, null);
  }
}
