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
package com.here.naksha.app.service.http.auth.actions;

import org.jetbrains.annotations.NotNull;

/**
 * Access to connectors.
 */
public enum XyzConnectorAction {
  /**
   * Allows to use connectors when creating or modifying a space. Not necessary to read features from a space using a connector.
   */
  ACCESS_CONNECTORS("accessConnectors"),

  /**
   * Allows to create, modify or delete connectors. Does not include the right to use the connector in a space!
   */
  MANAGE_CONNECTORS("manageConnectors");

  XyzConnectorAction(@NotNull String name) {
    this.name = name;
  }

  /**
   * The action name.
   */
  public final @NotNull String name;

  @Override
  public @NotNull String toString() {
    return name;
  }
}
