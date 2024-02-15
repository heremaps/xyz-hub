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
package com.here.naksha.lib.view;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.storage.IStorage;
import java.util.concurrent.Future;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class View implements IView {

  private Storage storage;

  private ViewLayerCollection viewLayerCollection;

  public View(@NotNull ViewLayerCollection viewLayerCollection) {
    this.viewLayerCollection = viewLayerCollection;
  }

  public View(Storage storage) {
    this.storage = storage;
  }

  public ViewLayerCollection getViewCollection() {
    return viewLayerCollection;
  }

  @Override
  public @NotNull ViewReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    return new ViewReadSession(this, context, useMaster);
  }

  @Override
  public @NotNull ViewWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    return new ViewWriteSession(this, context, useMaster);
  }

  @Override
  public @NotNull <T> Future<T> shutdown(@Nullable Fe1<T, IStorage> onShutdown) {
    throw new NotImplementedException();
  }

  @Override
  public void initStorage() {
    throw new UnsupportedOperationException("init all individual storages first");
  }

  @Override
  public void startMaintainer() {
    throw new NotImplementedException();
  }

  @Override
  public void maintainNow() {
    throw new NotImplementedException();
  }

  @Override
  public void stopMaintainer() {
    throw new NotImplementedException();
  }

  @Override
  public void setViewLayerCollection(ViewLayerCollection viewLayerCollection) {
    this.viewLayerCollection = viewLayerCollection;
  }
}
