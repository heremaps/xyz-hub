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
package com.here.naksha.lib.hub.mock;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.util.storage.RequestHelper.createFeatureRequest;

import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.hub.NakshaHubConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NHAdminMock implements IStorage {

  protected final @NotNull Map<String, Map<String, Object>> mockCollection;
  protected final @NotNull NakshaHubConfig nakshaHubConfig;

  public NHAdminMock(
      final @NotNull Map<String, Map<String, Object>> mockCollection, final @NotNull NakshaHubConfig customCfg) {
    this.mockCollection = mockCollection;
    this.nakshaHubConfig = customCfg;
    this.initStorage();
  }

  @Override
  public void initStorage() {
    final NakshaContext ctx = new NakshaContext().withAppId(nakshaHubConfig.appId);
    ctx.attachToCurrentThread();

    // Create all admin collections
    try (final IWriteSession admin = newWriteSession(ctx, true)) {
      final List<WriteOp<StorageCollection>> collectionList = new ArrayList<>();
      for (final String name : NakshaAdminCollection.ALL) {
        final StorageCollection collection = new StorageCollection(name);
        final WriteOp<StorageCollection> writeOp = new WriteOp<>(EWriteOp.INSERT, collection, false);
        collectionList.add(writeOp);
      }
      final Result wrResult = admin.execute(new WriteCollections<>(collectionList));
      if (wrResult == null) {
        admin.rollback();
        throw unchecked(new Exception("Unable to create Admin collections in Mock storage. Null result!"));
      } else if (wrResult instanceof ErrorResult er) {
        admin.rollback();
        throw unchecked(new Exception(
            "Unable to create Admin collections in Mock storage. " + er.toString(), er.exception));
      }
      admin.commit();
    }

    // Add custom config in naksha:configs
    try (final IWriteSession admin = newWriteSession(ctx, true)) {
      final Result wrResult = admin.execute(createFeatureRequest(
          NakshaAdminCollection.CONFIGS, nakshaHubConfig, IfExists.REPLACE, IfConflict.REPLACE));
      if (wrResult == null) {
        admin.rollback();
        throw unchecked(new Exception("Unable to add custom config in Mock storage. Null result!"));
      } else if (wrResult instanceof ErrorResult er) {
        admin.rollback();
        throw unchecked(
            new Exception("Unable to add custom config in Mock storage. " + er.toString(), er.exception));
      }
      admin.commit();
    }
  }

  /**
   * Starts the maintainer thread that will take about history garbage collection, sequencing and other background jobs.
   */
  @Override
  public void startMaintainer() {}

  /**
   * Blocking call to perform maintenance tasks right now. One-time maintenance.
   */
  @Override
  public void maintainNow() {}

  /**
   * Stops the maintainer thread.
   */
  @Override
  public void stopMaintainer() {}

  /**
   * Open a new write-session, optionally to a master-node (when being in a multi-writer cluster).
   *
   * @param context   the {@link NakshaContext} to which to link the session.
   * @param useMaster {@code true} if the master-node should be connected to; false if any writer is okay.
   * @return the write-session.
   */
  @Override
  public @NotNull IWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    return new NHAdminWriterMock(mockCollection);
  }

  /**
   * Open a new read-session, optionally to a master-node to prevent replication lags.
   *
   * @param context   the {@link NakshaContext} to which to link the session.
   * @param useMaster {@code true} if the master-node should be connected to, to avoid replication lag; false if any reader is okay.
   * @return the read-session.
   */
  @Override
  public @NotNull IReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    return new NHAdminReaderMock(mockCollection);
  }
}
