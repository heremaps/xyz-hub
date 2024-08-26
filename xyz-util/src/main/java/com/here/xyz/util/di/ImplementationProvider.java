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

package com.here.xyz.util.di;

import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public interface ImplementationProvider {
  Logger logger = LogManager.getLogger();

  /**
   * @return Whether this ImplementationProvider is suitable to be used in the current runtime environment
   */
  boolean chooseMe();

  static <T extends ImplementationProvider> T loadProvider(Class<T> superProvider) {
    ServiceLoader<T> loader = ServiceLoader.load(superProvider);

    List<T> availableProviders = loader.stream().map(Provider::get).collect(Collectors.toList());
    if (availableProviders.isEmpty())
      throw new RuntimeException("There are no implementations available for " + superProvider.getName());

    List<T> suitableProviders = availableProviders.stream().filter(provider -> provider.chooseMe()).collect(Collectors.toList());

    if (suitableProviders.isEmpty())
      throw new RuntimeException("No suitable implementation found for " + superProvider.getName() + ", available implementations: ["
          + providerNames(availableProviders) + "]");

    if (suitableProviders.size() > 1)
      throw new RuntimeException("Ambiguous implementation options. There exist multiple implementations being suitable for "
          + superProvider.getName() + ", suitable implementations: [" + providerNames(suitableProviders) + "]");

    return suitableProviders.get(0);
  }

  private static <T extends ImplementationProvider> String providerNames(List<T> providers) {
    return String.join(", ", providers.stream().map(impl -> impl.getClass().getName()).collect(Collectors.toList()));
  }
}