/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.util.db;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;

public final class AuroraAcuMonitorManager {

  private static final Logger logger = LogManager.getLogger();
  private static final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "acu-monitor");
        t.setDaemon(true);
        return t;
      });

  private static final ConcurrentMap<Region, CloudWatchClient> cwPerRegion = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, AuroraAcuMonitor> monitors = new ConcurrentHashMap<>();

  public static AuroraAcuMonitor get(String clusterId, Region region) {
    return monitors.computeIfAbsent(clusterId, id ->
        new AuroraAcuMonitor(id, region, executorService, cw(region)));
  }

  public static AuroraAcuMonitor getForClusterRole(String clusterId, String role, Region region) {
    try {
      AuroraAcuMonitor acuMonitor = monitors.computeIfAbsent(clusterId + ":" + role,
          key -> new AuroraAcuMonitor(clusterId, role, region, executorService, cw(region)));
      logger.info("Created role based ACU monitors for clusterId={} in {}.", clusterId, region);
      return acuMonitor;
    } catch (Exception e) {
      logger.warn("Could not create role based ACU monitors for clusterId={} in {}.", clusterId, region, e);
      return get(clusterId, region);
    }
  }

  public static void remove(String clusterId, String role) {
    if (clusterId == null) {
      return;
    }
    if (StringUtils.isBlank(role)) {
      AuroraAcuMonitor m = monitors.remove(clusterId);
      if (m != null) {
        m.stop();
      }
    } else {
      AuroraAcuMonitor m = monitors.remove(clusterId + ":" + role);
      if (m != null) {
        m.stop();
      }
    }
  }


  private static CloudWatchClient cw(Region r) {
    return cwPerRegion.computeIfAbsent(r, rr -> CloudWatchClient.builder().region(rr).build());
  }
}