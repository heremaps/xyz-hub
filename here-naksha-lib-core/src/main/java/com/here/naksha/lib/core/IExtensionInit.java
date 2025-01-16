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
package com.here.naksha.lib.core;

import com.here.naksha.lib.core.models.features.Extension;

/**
 * Naksha Extension Interface for all extensions providing initClassName.
 */
public interface IExtensionInit {

  /**
   * Initializes the extension with the specified hub and extension parameters.
   * This method should be called to set up any necessary resources or configurations
   * required by the extension to operate correctly.
   * @param hub The hub instance to be used by the extension.
   * @param extension Extension configuration supplied as part of deployment pipeline for respective Extension and sub-env.
   */
  void init(INaksha hub, Extension extension);

  /**
   * Closes the extension. This method should be called to ensure proper
   * cleanup when the extension is no longer needed.
   */
  void close();
}
