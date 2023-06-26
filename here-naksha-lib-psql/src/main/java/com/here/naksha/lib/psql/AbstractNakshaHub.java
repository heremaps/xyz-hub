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

import com.here.naksha.lib.core.INaksha;
import java.io.IOException;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;

/**
 * The abstract Naksha-Hub is the base class for the Naksha-Hub implementation, granting access to the administration PostgresQL database.
 * This is a special Naksha client, used to manage spaces, connectors, subscriptions and other administrative content. This client should
 * not be used to query data from a foreign storage, it only holds administrative spaces. Normally this is only created and used by the
 * Naksha-Hub itself and exposed to all other parts of the Naksha-Hub via the {@link INaksha#get()} method.
 */
public abstract class AbstractNakshaHub extends PsqlStorage implements INaksha {

  /**
   * Create a new Naksha client instance and register as default Naksha client.
   *
   * @param config the configuration of the admin-database to connect to.
   * @throws SQLException if any error occurred while accessing the database.
   * @throws IOException  if reading the SQL extensions from the resources fail.
   */
  protected AbstractNakshaHub(@NotNull PsqlConfig config) throws SQLException, IOException {
    super(config, 0L);
    instance.getAndSet(this);
  }
}
