/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub.util.metrics;

import static com.here.xyz.hub.util.metrics.base.Metric.MetricUnit.MILLISECONDS;

import com.here.xyz.hub.util.metrics.base.BareValuesMetric;
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//TODO: Refactor this metric into an AggregatingMetric!
public class GcDurationMetric extends BareValuesMetric {

  private static final Logger logger = LogManager.getLogger();
  private static AtomicReference<Collection<Double>> durations = new AtomicReference(new ArrayList<>());

  public GcDurationMetric(String metricName) {
    super(metricName, MILLISECONDS);
    registerBeanListener();
  }

  @Override
  protected Collection<Double> gatherValues() {
    return durations.getAndSet(new ArrayList<>());
  }

  static public void registerBeanListener() {
    //http://www.programcreek.com/java-api-examples/index.php?class=javax.management.MBeanServerConnection&method=addNotificationListener
    //https://docs.oracle.com/javase/8/docs/jre/api/management/extension/com/sun/management/GarbageCollectionNotificationInfo.html#GARBAGE_COLLECTION_NOTIFICATION
    for (GarbageCollectorMXBean gcMbean : ManagementFactory.getGarbageCollectorMXBeans()) {
      try {
        ManagementFactory.getPlatformMBeanServer().
            addNotificationListener(gcMbean.getObjectName(), listener, null, null);
      }
      catch (Exception e) {
        logger.error("Error while trying to gather Garbage Collector information for " + gcMbean.getName() + ".", e);
      }
    }
  }

  static private NotificationListener listener = (notification, handback) -> {
    if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
      //https://docs.oracle.com/javase/8/docs/jre/api/management/extension/com/sun/management/GarbageCollectionNotificationInfo.html
      CompositeData cd = (CompositeData) notification.getUserData();
      GarbageCollectionNotificationInfo gcNotificationInfo = GarbageCollectionNotificationInfo.from(cd);
      GcInfo gcInfo = gcNotificationInfo.getGcInfo();
      if (gcNotificationInfo.getGcAction().equals("end of major GC") || gcNotificationInfo.getGcAction().equals("end of GC cycle"))
        durations.get().add((double) gcInfo.getDuration());
    }
  };

}
