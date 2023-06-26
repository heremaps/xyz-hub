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
package com.here.naksha.lib.core.models;

import org.jetbrains.annotations.NotNull;

/**
 * A plugin is some code that implements an API and can be instantiated.
 *
 * @param <API> the api-type to create by the configuration.
 */
public interface IPlugin<API> {

  /**
   * Create a new instance of the plugin.
   *
   * @return the API.
   * @throws Exception if creating the instance failed.
   */
  @NotNull
  API newInstance() throws Exception;
}
