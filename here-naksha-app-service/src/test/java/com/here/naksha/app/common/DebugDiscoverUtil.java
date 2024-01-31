package com.here.naksha.app.common;

import java.lang.management.ManagementFactory;

public class DebugDiscoverUtil {

  private DebugDiscoverUtil() {
  }

  public static boolean isAppRunningOnDebug() {
    return ManagementFactory.getRuntimeMXBean().getInputArguments().toString().contains("-agentlib:jdwp");
  }
}
