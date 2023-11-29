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
package com.here.naksha.lib.hub;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.psql.PsqlInstanceConfig;
import com.here.naksha.lib.psql.PsqlStorage;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NakshaHubFactory {

  /**
   * Instantiate NakshaHub (INaksha compliant) instance by loading given {@link NakshaHubConfig#hubClassName hubClassName}. If config or
   * hubClassName is not provided (i.e. null), then default NakshaHub implementation will be used.
   *
   * @param instanceConfig configuration to be used for instantiating NakshaHub storage instance
   * @param config         The custom NakshaHub Config to be used
   * @param configId       The configId to be used for loading config from Storage (if custom config not provided)
   * @return NakshaHub (INaksha compliant) instance
   */
  public static @NotNull INaksha getInstance(
      final @Nullable String appName,
      final @Nullable PsqlInstanceConfig instanceConfig,
      final @Nullable NakshaHubConfig config,
      final @Nullable String configId,
      final @Nullable PsqlStorage.Params storageParams) {
    final String hubClassName = (config != null) ? config.hubClassName : NakshaHubConfig.defaultHubClassName();
    INaksha hub = null;
    try {
      final Class<?> theClass = Class.forName(hubClassName);
      if (INaksha.class.isAssignableFrom(theClass)) {
        final Constructor<?> constructor = theClass.getConstructor(
            String.class,
            PsqlInstanceConfig.class,
            NakshaHubConfig.class,
            String.class,
            PsqlStorage.Params.class);
        hub = (INaksha) constructor.newInstance(appName, instanceConfig, config, configId, storageParams);
      } else {
        throw unchecked(new Exception("Class '" + hubClassName + "' not INaksha compliant"));
      }
    } catch (ClassNotFoundException
        | InvocationTargetException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException ex) {
      throw unchecked(
          new Exception("Unable to instantiate INaksha implementation class '" + hubClassName + "'. ", ex));
    }
    return hub;
  }
}
