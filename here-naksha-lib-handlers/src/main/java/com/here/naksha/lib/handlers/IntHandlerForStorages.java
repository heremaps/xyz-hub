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
package com.here.naksha.lib.handlers;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import org.jetbrains.annotations.NotNull;

public class IntHandlerForStorages extends AdminFeatureEventHandler<Storage> {

  // TODO: addStorageIdToStreamInfo(PsqlStorage.ADMIN_STORAGE_ID, ctx);

  public IntHandlerForStorages(final @NotNull INaksha hub) {
    super(hub, Storage.class);
  }

  @Override
  protected @NotNull Result validateFeature(Storage storage) {
    if (storage.getClassName() == null || storage.getClassName().isEmpty()) {
      return new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "Mandatory parameter className missing!");
    }
    return new SuccessResult();
  }
}