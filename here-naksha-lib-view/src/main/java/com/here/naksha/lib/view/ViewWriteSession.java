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
package com.here.naksha.lib.view;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * It writes same value to all storages, the result must not be combined, to let clients know if operation succeeded in
 * each storage or not.
 */
public class ViewWriteSession extends ViewReadSession implements IWriteSession {

  IWriteSession session;
  ViewLayer writeLayer;
  final NakshaContext context;
  final boolean useMaster;

  public ViewWriteSession(@NotNull View viewRef, @Nullable NakshaContext context, boolean useMaster) {
    super(viewRef, context, useMaster);
    this.context = context;
    this.useMaster = useMaster;
  }

  public ViewWriteSession withWriteLayer(ViewLayer viewLayer) {
    if (this.session != null)
      throw new RuntimeException("Write session initiated with " + this.writeLayer.getCollectionId());
    this.writeLayer = viewLayer;
    return this;
  }

  public ViewWriteSession init() {
    if (writeLayer == null) writeLayer = viewRef.getViewCollection().getTopPriorityLayer();
    this.session = writeLayer.getStorage().newWriteSession(context, useMaster);
    return this;
  }
  /**
   * Executes write on one (top by default storage).
   *
   * @param writeRequest
   * @return
   */
  @Override
  public @NotNull Result execute(@NotNull WriteRequest<?, ?, ?> writeRequest) {
    if (!(writeRequest instanceof WriteFeatures)) {
      throw new UnsupportedOperationException("Only WriteFeatures are supported.");
    }
    getSession();
    ((WriteFeatures) writeRequest).setCollectionId(writeLayer.getCollectionId());
    return this.session.execute(writeRequest);
  }

  @Override
  public @NotNull IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return getSession().lockFeature(collectionId, featureId, timeout, timeUnit);
  }

  @Override
  public @NotNull IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return getSession().lockStorage(lockId, timeout, timeUnit);
  }

  @Override
  public void commit(boolean autoCloseCursors) {
    getSession().commit(autoCloseCursors);
  }

  @Override
  public void rollback(boolean autoCloseCursors) {
    getSession().rollback(true);
  }

  @Override
  public void close(boolean autoCloseCursors) {
    super.close();
    getSession().close();
  }

  private IWriteSession getSession() {
    if (this.session == null) init();
    return this.session;
  }
}
