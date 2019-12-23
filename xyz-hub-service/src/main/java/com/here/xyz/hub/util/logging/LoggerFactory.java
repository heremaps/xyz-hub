/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.util.logging;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;

final class LoggerFactory {

  private static Map<Object, Logger> loggers = new HashMap<>();

  static Logger getLogger(Class<?> clazz) {
    if (!loggers.containsKey(clazz)) {
      loggers.put(clazz, org.slf4j.LoggerFactory.getLogger(clazz));
    }
    return loggers.get(clazz);
  }

  public static Logger getLogger(String name) {
    if (!loggers.containsKey(name)) {
      loggers.put(name, org.slf4j.LoggerFactory.getLogger(name));
    }
    return loggers.get(name);
  }

  static void deregisterLogger(Object clazzOrName) {
    loggers.remove(clazzOrName);
  }

}
