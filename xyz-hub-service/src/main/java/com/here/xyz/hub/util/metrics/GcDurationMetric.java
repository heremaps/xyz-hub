package com.here.xyz.hub.util.metrics;

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

public class GcDurationMetric extends Metric {

  private static final Logger logger = LogManager.getLogger();
  private static AtomicReference<Collection<Double>> durations = new AtomicReference(new ArrayList<>());

  public GcDurationMetric(MetricPublisher publisher) {
    super(publisher, 30);
    registerBeanListener();
  }

  @Override
  Collection<Double> gatherValues() {
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
      if (gcNotificationInfo.getGcAction().equals("end of major GC"))
        durations.get().add((double) gcInfo.getDuration());
    }
  };

}
