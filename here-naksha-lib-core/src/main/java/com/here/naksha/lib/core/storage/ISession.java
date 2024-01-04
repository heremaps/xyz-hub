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
package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.NakshaVersion;
import javax.annotation.concurrent.NotThreadSafe;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Base interface of all session.
 */
@NotThreadSafe
@AvailableSince(NakshaVersion.v2_0_7)
public interface ISession extends AutoCloseable {

  /**
   * Closes the session, returns the underlying connection back to the connection pool. Any method of the session will from now on throw an
   * {@link IllegalStateException}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @Override
  void close();
}
