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

package com.here.xyz.hub.config.settings;

import com.here.xyz.hub.Service;
import java.util.Map;
import java.util.Map.Entry;

public class EnvironmentVariableOverrides extends SingletonSetting<Map<String, Object>> {

  public  EnvironmentVariableOverrides() {
    super();
  }

  /**
   * Applies all environment variable overrides contained in this setting object by setting / overwriting its values at the service
   * configuration object.
   */
  public void applyOverrides() throws VariableOverrideException {
    for (Entry<String, Object> e : data.entrySet())
      overrideServiceConfigurationValue(e.getKey(), e.getValue());
  }

  private void overrideServiceConfigurationValue(String variableName, Object overrideValue) throws VariableOverrideException {
    try {
      Service.configuration.getClass().getDeclaredField(variableName).set(Service.configuration, overrideValue);
    }
    catch (IllegalAccessException | NoSuchFieldException e) {
      throw new VariableOverrideException(variableName, "Error trying to override variable \"" + variableName + "\".", e);
    }
  }

  public static class VariableOverrideException extends Exception {

    public String variableName;

    public VariableOverrideException(String variableName, String message, Throwable cause) {
      super(message, cause);
      this.variableName = variableName;
    }
  }
}
