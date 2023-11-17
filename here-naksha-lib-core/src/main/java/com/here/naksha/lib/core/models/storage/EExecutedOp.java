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
package com.here.naksha.lib.core.models.storage;

import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * The operation that was actually executed.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_7)
public class EExecutedOp extends EStorageOp {

  /**
   * A read operation was executed.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp READ = def(EExecutedOp.class, "READ");

  /**
   * A write operation was performed, but the existing state was retained.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp RETAINED = def(EExecutedOp.class, "RETAINED");

  /**
   * A new feature was created.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp CREATED = def(EExecutedOp.class, "CREATED");

  /**
   * A feature was updated.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp UPDATED = def(EExecutedOp.class, "UPDATED");

  /**
   * The feature was deleted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp DELETED = def(EExecutedOp.class, "DELETED");

  /**
   * The feature was purged (eventually deleted, so that there is no further trace in the HEAD).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp PURGED = def(EExecutedOp.class, "PURGED");

  /**
   * The collection was restored, only possible for collections.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final EExecutedOp RESTORED = def(EExecutedOp.class, "RESTORED");

  @Override
  protected void init() {
    register(EExecutedOp.class);
  }
}
