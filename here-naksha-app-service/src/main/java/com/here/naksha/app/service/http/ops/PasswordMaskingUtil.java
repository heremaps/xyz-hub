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
package com.here.naksha.app.service.http.ops;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class PasswordMaskingUtil {
  private static final String JSON_KEY_PASSWORD = "password";
  private static final String PASSWORD_MASK = "xxxxxx";

  public static void removePasswordFromProps(Map<String, Object> propertiesAsMap) {
    for (Entry<String, Object> entry : propertiesAsMap.entrySet()) {
      if (Objects.equals(entry.getKey(), JSON_KEY_PASSWORD)) {
        entry.setValue(PASSWORD_MASK);
      } else if (entry.getValue() instanceof Map) {
        // recursive call to the nested json property
        removePasswordFromProps((Map<String, Object>) entry.getValue());
      } else if (entry.getValue() instanceof ArrayList array) {
        // recursive call to the nested array json
        for (Object arrayEntry : array) {
          removePasswordFromProps((Map<String, Object>) arrayEntry);
        }
      }
    }
  }
}
