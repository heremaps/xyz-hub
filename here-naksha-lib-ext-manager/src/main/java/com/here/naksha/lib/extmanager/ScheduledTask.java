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
package com.here.naksha.lib.extmanager;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.ExtensionConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledTask implements Runnable {
  private static final @NotNull Logger logger = LoggerFactory.getLogger(ExtensionCache.class);
  private final ExtensionCache extensionCache;
  private final INaksha naksha;

  public ScheduledTask(@NotNull ExtensionCache extensionCache, @NotNull INaksha naksha) {
    this.extensionCache = extensionCache;
    this.naksha = naksha;
  }

  @Override
  public void run() {
    while (true) {
      logger.info("Extension cache refresh job started");
      ExtensionConfig extensionConfig;
      long sleepMs = 0;
      try {
        extensionConfig = naksha.getExtensionConfig();
        sleepMs = extensionConfig.getExpiry() - System.currentTimeMillis();
        this.extensionCache.buildExtensionCache(extensionConfig);
      } catch (Exception e) {
        logger.error("Failed to refresh extension cache.", e);
      } finally {
        logger.info("Extension cache refresh job completed");
      }
      try {
        logger.info("Extension cache refresh job sleeps for " + sleepMs + " millisecond");
        if (sleepMs > 0) Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        logger.warn("Sleep of Extension refresh task got interrupted,", e);
      }
    }
  }
}
