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

import com.here.xyz.models.hub.Reader;
import io.vertx.core.Future;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class DynamoReaderConfigClient extends ReaderConfigClient {
  private static final Logger logger = LogManager.getLogger();

  public DynamoReaderConfigClient(String readers_dynamodb_table_arn) {
    super();
  }

  @Override
  protected Future<Reader> getReader(Marker marker, String spaceId, String readerId) {
    return null;
  }

  @Override
  protected Future<List<Reader>> getReaders(Marker marker, List<String> spaceIds) {
    return null;
  }

  @Override
  protected Future<Void> storeReader(Marker marker, Reader reader) {
    return null;
  }

  @Override
  protected Future<Reader> deleteReader(Marker marker, String spaceId, String reader) {
    return null;
  }

  @Override
  protected Future<Reader> deleteReaders(Marker marker, String spaceId) {
    return null;
  }

  @Override
  protected Future<List<Reader>> getAllReaders(Marker marker) {
    return null;
  }
}
