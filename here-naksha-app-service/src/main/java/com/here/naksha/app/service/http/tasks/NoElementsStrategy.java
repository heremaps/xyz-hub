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
package com.here.naksha.app.service.http.tasks;

import com.here.naksha.lib.core.models.XyzError;

public enum NoElementsStrategy {
  FAIL_ON_NO_ELEMENTS(
      XyzError.EXCEPTION, "Unexpected error while saving feature, the result cursor is empty / does not exist"),
  NOT_FOUND_ON_NO_ELEMENTS(XyzError.NOT_FOUND, "The desired feature does not exist.");

  final XyzError xyzError;
  final String message;

  NoElementsStrategy(XyzError xyzError, String message) {
    this.xyzError = xyzError;
    this.message = message;
  }
}
