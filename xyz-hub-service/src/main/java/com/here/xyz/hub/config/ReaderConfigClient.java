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

package com.here.xyz.hub.config;

import com.here.xyz.hub.Service;
import com.here.xyz.models.hub.Reader;
import io.vertx.core.Future;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public abstract class ReaderConfigClient implements Initializable{
  private static final Logger logger = LogManager.getLogger();

  public static ReaderConfigClient getInstance() {
    if (Service.configuration.READERS_DYNAMODB_TABLE_ARN != null) {
      return new DynamoReaderConfigClient(Service.configuration.READERS_DYNAMODB_TABLE_ARN);
    } else {
      return JDBCReaderConfigClient.getInstance();
    }
  }
  public abstract Future<Reader> getReader(Marker marker, String id, String spaceId);

  public abstract Future<List<Reader>> getReaders(Marker marker, String id, List<String> spaceIds);

  public abstract Future<List<Reader>> getReaders(Marker marker, String spaceId);

  public abstract Future<Void> storeReader(Marker marker, Reader reader);

  public abstract Future<Long> increaseVersion(Marker marker, String spaceId, String readerId, Long newVersion);

  public abstract Future<Reader> deleteReader(Marker marker, String id, String spaceId);

  public abstract Future<List<Reader>> deleteReaders(Marker marker, String spaceId);
}
