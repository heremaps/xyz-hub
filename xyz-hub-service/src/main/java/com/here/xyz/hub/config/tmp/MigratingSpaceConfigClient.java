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

package com.here.xyz.hub.config.tmp;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.DynamoSpaceConfigClient;
import com.here.xyz.hub.config.Initializable;
import com.here.xyz.hub.config.JDBCSpaceConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Marker;

public class MigratingSpaceConfigClient extends SpaceConfigClient {

  private static boolean initialized = false;
  private SpaceConfigClient newSpaceClient;
  private SpaceConfigClient oldSpaceClient;

  private MigratingSpaceConfigClient() {
    this.newSpaceClient = new DynamoSpaceConfigClient(Service.configuration.SPACES_DYNAMODB_TABLE_ARN);
    this.oldSpaceClient = JDBCSpaceConfigClient.getInstance();
  }

  @Override
  public synchronized void init(Handler<AsyncResult<Void>> onReady) {
    if (initialized) {
      onReady.handle(Future.succeededFuture());
      return;
    }

    initialized = true;

    CompositeFuture.all(
        initFuture(oldSpaceClient),
        initFuture(newSpaceClient)
    ).setHandler(h -> onReady.handle(Future.succeededFuture()));
  }

  private <T> Future<T> initFuture(Initializable objectToInit) {
    return Future.future(h -> {
      objectToInit.init(h2 -> {
        if (h2.failed()) {
          h.handle(Future.failedFuture(h2.cause()));
        } else {
          h.handle(Future.succeededFuture());
        }
      });
    });
  }

  @Override
  protected void getSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    // i know this is pretty bad logic, anyways, this is just temporary...
    newSpaceClient.get(marker, spaceId, ar -> {
      if (ar.failed() || ar.result() == null) {
        oldSpaceClient.get(marker, spaceId, oldAr -> {
          if (oldAr.failed()) {
            handler.handle(oldAr);
          } else {
            Space space = oldAr.result();
            if (space != null) {
              moveSpace(marker, space, migrationResult -> {
              });
            }
            handler.handle(oldAr);
          }
        });
      } else {
        handler.handle(ar);
      }
    });
  }

  @Override
  protected void storeSpace(Marker marker, Space space, Handler<AsyncResult<Space>> handler) {
    newSpaceClient.store(marker, space, handler);
  }

  @Override
  protected void deleteSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    oldSpaceClient.delete(marker, spaceId, ar -> {
      if (ar.failed()) {
        handler.handle(ar);
      } else {
        newSpaceClient.delete(marker, spaceId, handler);
      }
    });
  }

  @Override
  protected void getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition,
      Handler<AsyncResult<List<Space>>> handler) {
    oldSpaceClient.getSelected(marker, authorizedCondition, selectedCondition, ar -> {
      if (ar.failed()) {
        handler.handle(ar);
      } else {
        newSpaceClient.getSelected(marker, authorizedCondition, selectedCondition, newAr -> {
          if (newAr.failed()) {
            handler.handle(newAr);
          } else {
            List<Space> spaces = new ArrayList<>(ar.result());
            spaces.addAll(newAr.result());
            handler.handle(Future.succeededFuture(spaces));
          }
        });
      }
    });
  }

  private void moveSpace(Marker marker, Space space, Handler<AsyncResult<Space>> handler) {
    newSpaceClient.store(marker, space, storeResult -> {
      if (storeResult.failed()) {
        logger().error(marker, "Error when trying to store space while migrating it. Space ID: " + space.getId(), storeResult.cause());
        return;
      }
      oldSpaceClient.delete(marker, space.getId(), deletionResult -> {
        if (deletionResult.failed()) {
          logger().error(marker, "Error when trying to delete old space while migrating it. Space ID: " + space.getId(),
              deletionResult.cause());
          return;
        }
        handler.handle(Future.succeededFuture(space));
      });
    });
  }
}
