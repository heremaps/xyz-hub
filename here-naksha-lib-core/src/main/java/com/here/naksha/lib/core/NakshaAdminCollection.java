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
package com.here.naksha.lib.core;

import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * All well-known collections of the Naksha-Hub itself. Still, not all Naksha-Hubs may support them, for example the Naksha extension
 * library currently does not support any collection out of the box!
 */
public final class NakshaAdminCollection {

  /**
   * The admin schema.
   */
  public static final String SCHEMA = "naksha";

  /**
   * The Naksha-Hub configurations.
   */
  public static final String CONFIGS = "naksha:configs";

  /**
   * The collections for all spaces.
   */
  public static final String SPACES = "naksha:spaces";

  /**
   * The collections for all subscriptions.
   */
  public static final String SUBSCRIPTIONS = "naksha:subscriptions";

  /**
   * The collections for all connectors.
   */
  public static final String EVENT_HANDLERS = "naksha:event_handlers";

  /**
   * The collections for all storages.
   */
  public static final String STORAGES = "naksha:storages";

  /**
   * The collections for all extensions.
   */
  public static final String EXTENSIONS = "naksha:extensions";

  /**
   * List of all admin-db collections.
   */
  public static final List<@NotNull String> ALL =
      List.of(CONFIGS, SPACES, SUBSCRIPTIONS, EVENT_HANDLERS, STORAGES, EXTENSIONS);
}
