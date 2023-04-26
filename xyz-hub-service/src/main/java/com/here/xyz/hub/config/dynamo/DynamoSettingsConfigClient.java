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

package com.here.xyz.hub.config.dynamo;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.here.xyz.hub.config.SettingsConfigClient;
import com.here.xyz.hub.config.settings.Setting;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.jackson.DatabindCodec;
import java.util.Map;
import org.apache.logging.log4j.Marker;

public class DynamoSettingsConfigClient extends SettingsConfigClient {

  private DynamoClient dynamoClient;
  private Table settings;

  public DynamoSettingsConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn);
    logger.info("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    settings = dynamoClient.getTable();
  }

  @Override
  protected Future<Setting> getSetting(Marker marker, String settingId) {
    logger.info(marker, "Getting setting with ID: {}", settingId);
    return DynamoClient.dynamoWorkers.executeBlocking(p -> {
      try {
        Item settingItem = settings.getItem("id", settingId);
        if (settingItem == null) {
          logger.info(marker, "Getting setting with ID: {} returned null", settingId);
          p.complete();
        }
        else {
          Map<String, Object> itemData = settingItem.asMap();
          final Setting setting = DatabindCodec.mapper().convertValue(itemData, Setting.class);
          if (setting != null)
            logger.info(marker, "Setting ID: {} has been decoded", settingId);
          else
            logger.info(marker, "Setting ID: {} has been decoded to null", settingId);
          p.complete(setting);
        }
      }
      catch (Exception e) {
        p.fail(e);
      }
    });
  }

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    if (dynamoClient.isLocal()) {
      logger.info("DynamoDB running locally, initializing tables.");

      try {
        dynamoClient.createTable(settings.getTableName(), "id:S", "id", null, null);
      }
      catch (AmazonDynamoDBException e) {
        logger.error("Failure during creating table on DynamoSettingsConfigClient init", e);
        onReady.handle(Future.failedFuture(e));
        return;
      }
    }
    onReady.handle(Future.succeededFuture());
  }
}
