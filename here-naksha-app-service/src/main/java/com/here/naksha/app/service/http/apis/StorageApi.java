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
package com.here.naksha.app.service.http.apis;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.models.features.Storage;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.storage.*;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class StorageApi extends Api {

  public StorageApi(final @NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(final @NotNull RouterBuilder rb) {
    rb.operation("getStorages").handler(this::getStorages);
    rb.operation("postStorage").handler(this::createStorage);
  }

  @Override
  public void addManualRoutes(final @NotNull Router router) {}

  private void getStorages(final @NotNull RoutingContext routingContext) {
    app().executeTask(() -> {
      try (final IReadTransaction tx = naksha().storage().openReplicationTransaction(naksha().settings())) {
        // TODO HP : Fix adding a new Storage
        /*
        final IResultSet<Storage> rs = tx.readFeatures(Storage.class, NakshaAdminCollection.STORAGES)
        .getAll(0, Integer.MAX_VALUE);
        */
        final IResultSet<Storage> rs = null;
        final List<@NotNull Storage> featureList = rs.toList(0, Integer.MAX_VALUE);
        rs.close();
        final XyzFeatureCollection response = new XyzFeatureCollection();
        response.setFeatures(featureList);
        verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE_COLLECTION, response);
        return response;
      }
    });
  }

  private void createStorage(final @NotNull RoutingContext routingContext) {
    app().executeTask(() -> {
      Storage storage = null;
      // Read request JSON
      try (final Json json = Json.get()) {
        final String bodyJson = routingContext.body().asString();
        storage = json.reader(ViewDeserialize.User.class)
            .forType(Storage.class)
            .readValue(bodyJson);
      } catch (Exception e) {
        throw unchecked(e);
      }
      // Insert storage in database
      try (final IMasterTransaction tx = naksha().storage().openMasterTransaction(naksha().settings())) {
        // TODO HP : Fix adding a new Storage
        /*
        final ModifyFeaturesResp modifyResponse = tx.writeFeatures(
        Storage.class, NakshaAdminCollection.STORAGES)
        .modifyFeatures(new ModifyFeaturesReq<Storage>(true).insert(storage));
        */
        final ModifyFeaturesResp modifyResponse = null;
        tx.commit();
        final XyzFeatureCollection response = verticle.transformModifyResponse(modifyResponse);
        verticle.sendXyzResponse(routingContext, HttpResponseType.FEATURE_COLLECTION, response);
        return response;
      }
    });
  }
}
