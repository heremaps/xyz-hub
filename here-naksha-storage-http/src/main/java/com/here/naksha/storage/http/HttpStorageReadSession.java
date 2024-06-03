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
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpStorageReadSession implements IReadSession {

  private static final Logger log = LoggerFactory.getLogger(HttpStorageReadSession.class);

  @NotNull
  private final NakshaContext context;

  private final boolean useMaster;

  @NotNull
  private final RequestSender requestSender;

  HttpStorageReadSession(@Nullable NakshaContext context, boolean useMaster, @NotNull RequestSender requestSender) {
    this.context = context == null ? NakshaContext.currentContext() : context;
    this.useMaster = useMaster;
    this.requestSender = requestSender;
  }

  @Override
  public boolean isMasterConnect() {
    return useMaster;
  }

  @Override
  public @NotNull NakshaContext getNakshaContext() {
    return context;
  }

  @Override
  public int getFetchSize() {
    throw new NotImplementedException();
  }

  @Override
  public void setFetchSize(int size) {
    throw new NotImplementedException();
  }

  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    throw new NotImplementedException();
  }

  @Override
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    throw new NotImplementedException();
  }

  @Override
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    throw new NotImplementedException();
  }

  @Override
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    throw new NotImplementedException();
  }

  @Override
  public @NotNull Result execute(@NotNull ReadRequest<?> readRequest) {
    try {
      return HttpStorageReadExecute.execute(context, (ReadFeaturesProxyWrapper) readRequest, requestSender);
    } catch (Exception e) {
      log.warn("We got exception while executing Read request.", e);
      return new ErrorResult(XyzError.EXCEPTION, e.getMessage(), e);
    }
  }

  @Override
  public @NotNull Result process(@NotNull Notification<?> notification) {
    throw new NotImplementedException();
  }

  @Override
  public void close() {}
}
