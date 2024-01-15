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
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IWriteSession;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * It writes same value to all storages, the result must not be combined, to let clients know if operation succeeded in
 * each storage or not.
 */
// FIXME it's abstract only to not implement all IReadSession methods at the moment
public abstract class ViewWriteSession extends ViewReadSession implements IWriteSession {

  public ViewWriteSession(@NotNull View viewRef, @Nullable NakshaContext context, boolean useMaster) {
    super(viewRef, context, useMaster);
  }

  /**
   * Executes write on one (top by default storage).
   *
   * @param writeRequest
   * @return
   */
  @Override
  public @NotNull Result execute(@NotNull WriteRequest<?, ?, ?> writeRequest) {
    throw new NotImplementedException();
  }
}
