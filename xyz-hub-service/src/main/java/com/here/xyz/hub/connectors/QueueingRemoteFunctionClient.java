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

package com.here.xyz.hub.connectors;

import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.ByteSizeAware;
import com.here.xyz.hub.util.LimitedQueue;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public abstract class QueueingRemoteFunctionClient extends RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();

  /**
   * The global maximum byte size that is available for allocation by all of the queues.
   */
  public static final long GLOBAL_MAX_QUEUE_BYTE_SIZE = (long) Service.configuration.GLOBAL_MAX_QUEUE_SIZE * 1024 * 1024;

  /**
   * Tweaking constant for the percentage that the connection slots relevance should be used. The rest is the rateOfService relevance.
   */
  public static final float CONNECTION_SLOTS_RELEVANCE = 0.5f;
  public static final float REQUEST_RELEVANCE_FACTOR = 100;
  private static final int SIZE_ADJUSTMENT_INTERVAL = 3000; //3 seconds
  private static Set<QueueingRemoteFunctionClient> clientInstances = new HashSet<>();
  private static LongAdder globalMinConnectionSum = new LongAdder();
  private static AtomicLong lastSizeAdjustment;
  AtomicInteger usedConnections = new AtomicInteger(0);
  /**
   * Sliding average request execution time in seconds.
   */
  private double SARET = 1d; // 1 second initial value
  /**
   * An approximation for the maximum number of requests per second which can be executed based on the performance of the remote function.
   */
  private double rateOfService;
  private LimitedQueue<FunctionCall> queue = new LimitedQueue<>(0, 0);


  public QueueingRemoteFunctionClient(Connector connectorConfig) {
    super(connectorConfig);
    recalculateRateOfService();

    clientInstances.add(this);
    globalMinConnectionSum.add(getMinConnections());

    /*
    NOTE: For simplicity the queues will only get a fix maxByteSize which is dependent by their
    priority (minConnection-ratio) so now we're just doing the queue size (re-)adjustment for all queues once whenever a
    new RemoteFunctionClient get's created.
    However, this behavior will be optimized in the future.
     */
    adjustQueueByteSizes();
    /*
    For simplicity also here just set the value to maximum long. That means that we don't take the performance of the
    connector into account for defining the maximum queue length. Doing so this would just be a performance- / cost-
    optimization.
     */
    queue.setMaxSize(Long.MAX_VALUE);
  }

  private static boolean compareAndIncrementUpTo(int maxExpect, AtomicInteger i) {
    int currentValue = i.get();
    while (currentValue < maxExpect) {
      if (i.compareAndSet(currentValue, currentValue + 1)) {
        return true;
      }
      currentValue = i.get();
    }
    return false;
  }

  /**
   * (Re-)Adjusts the maximum byte sizes of the queues of all existing RemoteFunctionClients. When calling this method while there are
   * requests in the queues this could result in discards of those requests.
   */
  private static void adjustQueueByteSizes() {
    //NOTE: For simplicity the queues will only get a fix maxByteSize which is dependent by their priority
    //TODO: Improve the calculation with respect to the throughput and do the adjustments when necessary at run-time
    clientInstances.stream().forEach(c -> c.queue.setMaxByteSize((long) (c.getPriority() * GLOBAL_MAX_QUEUE_BYTE_SIZE)));
  }

  public static long getGlobalUsedQueueMemory() {
    return clientInstances.stream().mapToLong(c -> c.queue.getByteSize()).sum();
  }

  public static double getGlobalThroughput() {
    return clientInstances.stream().mapToDouble(c -> c.getThroughput()).sum();
  }

  public static double getGlobalArrivalRate() {
    return clientInstances.stream().mapToDouble(c -> c.getArrivalRate()).sum();
  }

  public static long getGlobalMaxConnections() {
    return clientInstances.stream().mapToLong(c -> c.getMaxConnections()).sum();
  }

  public static long getGlobalUsedConnections() {
    return clientInstances.stream().mapToLong(c -> c.getUsedConnections()).sum();
  }

  public static Set<QueueingRemoteFunctionClient> getInstances() {
    return Collections.unmodifiableSet(clientInstances);
  }

  @Override
  protected void submit(final Marker marker, byte[] bytes, final Handler<AsyncResult<byte[]>> callback) {
    Handler<AsyncResult<byte[]>> cb = r -> {
      //This is the point where the request's response came back so measure the throughput
      invokeCompleted();
      callback.handle(r);
    };

    //This is the point where new requests arrive so measure the arrival time
    invokeStarted();

    if (!compareAndIncrementUpTo(getMaxConnections(), usedConnections)) {
      enqueue(marker, bytes, cb);
      return;
    }
    _invoke(marker, bytes, cb);
  }

  private void _invoke(final Marker marker, byte[] bytes, final Handler<AsyncResult<byte[]>> callback) {
    //long start = System.nanoTime();
    invoke(marker, bytes, r -> {
      //long end = System.nanoTime();
      //TODO: Activate performance calculation once it's implemented completely
      //recalculatePerformance(end - start, TimeUnit.NANOSECONDS);
      //Look into queue if there is something further to do
      FunctionCall fc = queue.remove();
      if (fc == null) {
        usedConnections.getAndDecrement(); //Free the connection only in case it's not needed for the next invocation
      }
      try {
        callback.handle(r);
      } catch (Exception e) {
        logger.error(marker, "Error while calling response handler", e);
      }
      //In case there has been an enqueued element invoke the it
      if (fc != null) {
        _invoke(fc.marker, fc.bytes, fc.callback);
      }
    });
  }

  private void recalculatePerformance(long executionTime, TimeUnit timeUnit) {
    recalculateSARET(executionTime, timeUnit);
    recalculateRateOfService();
    adjustQueueElementCount();
  }

  private void recalculateSARET(long executionTime, TimeUnit timeUnit) {
    double executionTimeSeconds = (double) (timeUnit.toMicros(executionTime)) / 1_000_000d;
    double requestRelevance = 1 / (rateOfService * REQUEST_RELEVANCE_FACTOR);
    SARET = executionTimeSeconds * requestRelevance + SARET * (1d - requestRelevance);


  }

  public void recalculateRateOfService() {
    rateOfService = getMaxConnections() / SARET;
  }

  public double getRateOfService() {
    return rateOfService;
  }

  public int getMinConnections() {
    return connectorConfig.getMinConnectionsPerInstance();
  }

  public int getMaxConnections() {
    return connectorConfig.getMaxConnectionsPerInstance();
  }

  public int getUsedConnections() {
    return usedConnections.intValue();
  }

  public double getPriority() {
    return (double) getMinConnections() / globalMinConnectionSum.doubleValue();
  }

  public long getMaxQueueSize() {
    return queue.getMaxSize();
  }

  public long getQueueSize() {
    return queue.getSize();
  }

  public long getMaxQueueByteSize() {
    return queue.getMaxByteSize();
  }

  public long getQueueByteSize() {
    return queue.getByteSize();
  }

  /**
   * Sets the maximum feasible element count of the queue with respect to the {@link #REQUEST_TIMEOUT} and the {@link #rateOfService} of
   * this RemoteFunctionClient.
   */
  private void adjustQueueElementCount() {
    long maxFeasibleElements = (long) Math.ceil(rateOfService * REQUEST_TIMEOUT);
    queue.setMaxSize(maxFeasibleElements);
  }

  private void enqueue(final Marker marker, byte[] bytes, final Handler<AsyncResult<byte[]>> callback) {
    FunctionCall fc = new FunctionCall(marker, bytes, callback);

    /*if (System.currentTimeMillis() > lastSizeAdjustment.get() + SIZE_ADJUSTMENT_INTERVAL
        && fc.getByteSize() + queue.getByteSize() > queue.getMaxByteSize()) {
      //Element won't fit into queue so we try to enlarge it
      adjustQueueByteSizes();
    }*/

    //In any case add the element to the queue
    queue.add(fc)
        .stream()
        //Send timeout for discarded (old) calls
        .forEach(timeoutFc ->
            timeoutFc.callback
                .handle(Future.failedFuture(new HttpException(TOO_MANY_REQUESTS, "Remote function is busy or cannot be invoked."))));
  }

  public static class FunctionCall implements ByteSizeAware {

    final Marker marker;
    final byte[] bytes;
    final Handler<AsyncResult<byte[]>> callback;

    public FunctionCall(Marker marker, byte[] bytes, Handler<AsyncResult<byte[]>> callback) {
      this.marker = marker;
      this.bytes = bytes;
      this.callback = callback;
    }

    @Override
    public long getByteSize() {
      return bytes.length;
    }
  }
}
