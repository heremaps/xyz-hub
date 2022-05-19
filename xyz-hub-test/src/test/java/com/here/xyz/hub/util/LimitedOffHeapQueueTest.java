package com.here.xyz.hub.util;

public class LimitedOffHeapQueueTest extends LimitedQueueTest {

  @Override
  protected Class<? extends LimitedQueue> getQueueClass() {
    return LimitedOffHeapQueue.class;
  }

}
