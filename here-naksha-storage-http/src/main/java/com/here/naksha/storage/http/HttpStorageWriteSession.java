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
package com.here.naksha.storage.http;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.storage.http.connector.ConnectorInterfaceWriteExecute;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpStorageWriteSession extends HttpStorageReadSession implements IWriteSession {
  private static final Logger log = LoggerFactory.getLogger(HttpStorageWriteSession.class);
  private final HttpInterface httpInterface;

  public HttpStorageWriteSession(NakshaContext context, RequestSender requestSender, HttpInterface httpInterface) {
    super(context, true, requestSender, httpInterface);
    this.httpInterface = httpInterface;
  }

  @Override
  public @NotNull Result execute(@NotNull WriteRequest<?, ?, ?> writeRequest) {
    try {
      return switch (httpInterface) {
        case ffwAdapter -> new ErrorResult(
            XyzError.NOT_IMPLEMENTED, "Writing not supported by underlying storage");
        case dataHubConnector -> new ConnectorInterfaceWriteExecute(
                getNakshaContext(), (WriteXyzFeatures) writeRequest, getRequestSender())
            .execute();
      };
    } catch (ConnectorInterfaceWriteExecute.ConflictException e) {
      return new ErrorResult(XyzError.CONFLICT, e.getMessage(), e);
    } catch (UnsupportedOperationException e) {
      return new ErrorResult(XyzError.NOT_IMPLEMENTED, e.getMessage(), e);
    } catch (Exception e) {
      log.warn("We got exception while executing Write request.", e);
      return new ErrorResult(XyzError.EXCEPTION, e.getMessage(), e);
    }
  }

  @Override
  public @NotNull IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return null;
  }

  @Override
  public @NotNull IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return null;
  }

  @Override
  public void commit(boolean autoCloseCursors) {}

  @Override
  public void rollback(boolean autoCloseCursors) {}

  @Override
  public void close(boolean autoCloseCursors) {}
}
