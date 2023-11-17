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
package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import java.sql.Connection;
import org.jetbrains.annotations.NotNull;

@Deprecated
public final class PsqlReadSession extends PsqlSession {

  PsqlReadSession(@NotNull PostgresStorage storage, @NotNull NakshaContext context, @NotNull Connection connection) {
    super(storage, context, connection, true);
  }

  @Override
  public @NotNull Result execute(@NotNull ReadRequest<?> readRequest) {
    return session().executeRead(readRequest);
  }

  @Override
  public @NotNull Result process(@NotNull Notification<?> notification) {
    return session().process(notification);
  }
}
