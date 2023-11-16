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

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteOpResult;
import java.util.List;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MockWriteResult<T extends XyzFeature> extends SuccessResult {

  private final @NotNull Class<T> featureType;
  private final @NotNull List<WriteOpResult<T>> writeOpResults;

  public MockWriteResult(@NotNull Class<T> featureType, @NotNull List<WriteOpResult<T>> writeOpResults) {
    this.featureType = featureType;
    this.writeOpResults = writeOpResults;
    this.cursor = new WriteOpResultCursor();
  }

  class WriteOpResultCursor extends MockResultCursor<T> {

    WriteOpResultCursor() {
      super(featureType, (List) writeOpResults);
    }

    @Override
    public @Nullable T getFeature() throws NoSuchElementException {
      if (!isPositionValid()) {
        throw new NoSuchElementException();
      }
      return writeOpResults.get(currentPos).feature;
    }
  }
}
