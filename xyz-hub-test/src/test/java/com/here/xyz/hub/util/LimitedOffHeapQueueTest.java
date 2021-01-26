package com.here.xyz.hub.util;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.Service.Config;
import org.junit.BeforeClass;

public class LimitedOffHeapQueueTest extends LimitedQueueTest {

  @Override
  protected Class<? extends LimitedQueue> getQueueClass() {
    return LimitedOffHeapQueue.class;
  }

}
