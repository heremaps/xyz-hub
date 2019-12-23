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

import org.slf4j.Logger;
import org.slf4j.helpers.Util;

/**
 * This interface should be added to the list of implemented interfaces of classes which should have logging support. Doing so, instances of
 * that class can simply get a cached logger object for the class by calling the {@link #getLogger()} method.
 */
public interface Logging {

  static Logger getLogger() {
    return LoggerFactory.getLogger(Util.getCallingClass());
  }

  default Logger logger() {
    return LoggerFactory.getLogger(this.getClass());
  }

  default void destroyLogger() {
    LoggerFactory.deregisterLogger(this.getClass());
  }

}
