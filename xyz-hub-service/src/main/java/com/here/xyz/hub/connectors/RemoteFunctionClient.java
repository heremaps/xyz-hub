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

import com.google.common.io.ByteStreams;
import com.here.xyz.Payload;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.ByteSizeAware;
import com.here.xyz.hub.util.LimitedQueue;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.ConcurrentHashSet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.GZIPInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public abstract class RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();

  public static final long REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(Service.configuration.REMOTE_FUNCTION_REQUEST_TIMEOUT);
  private static int MEASUREMENT_INTERVAL = 1000; //1s

  protected Connector connectorConfig;

  private final LongAdder requestsSinceLastArrivalRateMeasurement = new LongAdder();
  private final AtomicLong lastArrivalRateMeasurement = new AtomicLong(Service.currentTimeMillis());

  private final LongAdder responsesSinceLastThroughputMeasurement = new LongAdder();
  private final AtomicLong lastThroughputMeasurement = new AtomicLong(Service.currentTimeMillis());

  /**
   * The number of requests per second currently being executed by this RemoteFunctionClient.
   */
  private volatile double throughput;

  /**
   * The number of requests per second currently being submitted to this RemoteFunctionClient.
   */
  private volatile double arrivalRate;

  /**
   * The global maximum byte size that is available for allocation by all of the queues.
   */
  public static final long GLOBAL_MAX_QUEUE_BYTE_SIZE = (long) Service.configuration.GLOBAL_MAX_QUEUE_SIZE * 1024 * 1024;

//  /**
//   * Tweaking constant for the percentage that the connection slots relevance should be used. The rest is the rateOfService relevance.
//   */
//  public static final float CONNECTION_SLOTS_RELEVANCE = 0.5f;
//  public static final float REQUEST_RELEVANCE_FACTOR = 100;
//  private static final int SIZE_ADJUSTMENT_INTERVAL = 3000; //3 seconds

  /**
   * All instances that were created and are active.
   */
  private static Set<RemoteFunctionClient> clientInstances = new ConcurrentHashSet<>();
  private static LongAdder globalMinConnectionSum = new LongAdder();
  private static AtomicLong lastSizeAdjustment;
  AtomicInteger usedConnections = new AtomicInteger(0);

//  /**
//   * Sliding average request execution time in seconds.
//   */
//  private double SARET = 1d; // 1 second initial value
  /**
   * An approximation for the maximum number of requests per second which can be executed based on the performance of the remote function.
   */
  private double rateOfService;
  private LimitedQueue<FunctionCall> queue = new LimitedQueue<>(0, 0);

  RemoteFunctionClient(Connector connectorConfig) {
    if (connectorConfig == null) {
      throw new NullPointerException();
    }
    setConnectorConfig(connectorConfig);

//    recalculateRateOfService();

   /*
    For simplicity also here just set the value to maximum long. That means that we don't take the performance of the
    connector into account for defining the maximum queue length. Doing so this would just be a performance- / cost-
    optimization.
     */
    queue.setMaxSize(Long.MAX_VALUE);

    initialize();
    //NOTE: This must be done as last construction step of instances of this class
    clientInstances.add(this);
  }

  /**
   * This method does further initialization for this client.
   * It's guaranteed to be called before an instance will be actually used by not adding it to the internal {@link #clientInstances} map
   * before initialization.
   * Sub-classes should override that method to do further initialization rather than doing that in their constructor.
   *
   * @throws NullPointerException if the connector configuration is null.
   */
  protected void initialize() {}

  protected static void checkResponseSize(byte[] response) throws HttpException {
    boolean isGZIP = response != null && response.length >= 2
        && GZIPInputStream.GZIP_MAGIC == (((int) response[0] & 0xff) | ((response[1] << 8) & 0xff00));

    assert response != null;
    if (isGZIP && response.length > Api.MAX_COMPRESSED_RESPONSE_LENGTH || response.length > Api.MAX_RESPONSE_LENGTH) {
      throw new HttpException(Api.RESPONSE_PAYLOAD_TOO_LARGE, Api.RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE);
    }
  }

  /**
   * Should be overridden in sub-classes to implement clean-up steps (e.g. closing client / connections).
   */
  void destroy() {
    if (connectorConfig != null) {
      clientInstances.remove(this);
      globalMinConnectionSum.add(getMinConnections());
      adjustQueueByteSizes();
    }
  }

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

  /**
   * Measures the occurrence of events in relation to the time having passed by since the last measurement. This is at minimum the value of
   * {@link #MEASUREMENT_INTERVAL}.
   *
   * @param eventCount A counter for the events having occurred so far within the current time-interval
   * @param lastMeasurementTime The point in time when the last measurement was done This reference will be updated in case the
   * time-interval was exceeded
   * @return The new current value for the dimension. If the time-interval was exceeded this is a newly calculated value otherwise the
   * return value is -1.
   */
  protected final double measureDimension(LongAdder eventCount, AtomicLong lastMeasurementTime) {
    long now = Service.currentTimeMillis();
    long last = lastMeasurementTime.get();
    if (now - last > MEASUREMENT_INTERVAL) {
      //Only if this thread was the one setting the new measurement timestamp it may be the one resetting the event counter
      if (lastMeasurementTime.compareAndSet(last, now)) {
        long evtSum = eventCount.sum();
        //"Reset" the adder by subtracting the current evtSum (We can't use #reset() as this isn't thread safe)
        eventCount.add(-evtSum);
        //Calculate the new dimension value
        return (double) evtSum / ((double) (now - last) / 1000d);
      }
    }
    return -1;
  }

  protected final void invokeStarted() {
    requestsSinceLastArrivalRateMeasurement.increment();
    measureArrival();
  }

  private void measureArrival() {
    double newAR = measureDimension(requestsSinceLastArrivalRateMeasurement, lastArrivalRateMeasurement);
    if (newAR != -1) {
      arrivalRate = newAR;
    }
  }

  protected final void invokeCompleted() {
    responsesSinceLastThroughputMeasurement.increment();
    measureThroughput();
  }

  private void measureThroughput() {
    double newTP = measureDimension(responsesSinceLastThroughputMeasurement, lastThroughputMeasurement);
    if (newTP != -1) {
      throughput = newTP;
    }
  }

  protected abstract void invoke(final Marker marker, byte[] bytes, final Handler<AsyncResult<byte[]>> callback);

  public double getThroughput() {
    measureThroughput();
    return throughput;
  }

  public double getArrivalRate() {
    measureArrival();
    return arrivalRate;
  }

  public Connector getConnectorConfig() {
    return connectorConfig;
  }

  /**
   * Should be called when the connector configuration changed during the runtime in order to inform this function client to do necessary
   * update steps.
   * Should be overridden in sub-classes to implement refreshing steps. (e.g. creating client / connections)
   *
   * @param connectorConfig the connector configuration
   * @throws NullPointerException if the given connector configuration is null.
   * @throws IllegalArgumentException if the given connector configuration is null or the id of the current connector configuration and the
   *  given one do not match.
   */
  synchronized void setConnectorConfig(Connector connectorConfig) throws NullPointerException, IllegalArgumentException {
    if (connectorConfig == null) {
      throw new NullPointerException("connectorConfig");
    }
    if (this.connectorConfig != null && !this.connectorConfig.id.equals(connectorConfig.id)) throw new IllegalArgumentException(
        "Wrong connector config was provided to an existing function client during a runtime update. IDs are not matching. new ID: "
            + connectorConfig.id + " vs. old ID: " + this.connectorConfig.id);

    final int oldMinConnections = getMinConnections();
    this.connectorConfig = connectorConfig;
    globalMinConnectionSum.add(getMinConnections() - oldMinConnections);
    adjustQueueByteSizes();
  }

//  /**
//   *
//   * @param currentValue The current value of the sliding average of the dimension
//   * @param slideInValue The value to take into account for the new average additionally
//   * @param slideInRelevance A number between 0 .. 1 indicating the relevance of the slideInValue in relation to the
//   *  current value of the sliding average.
//   * @return The new value of the sliding average
//   */
//  @SuppressWarnings("unused")
//  protected double calculateSlidingAverage(double currentValue, double slideInValue, double slideInRelevance) {
//    return currentValue * (1d - slideInRelevance) + slideInValue * slideInRelevance;
//  }

  protected byte[] getDecompressed(final byte[] bytes) throws IOException {
    return ByteStreams.toByteArray(Payload.prepareInputStream(new ByteArrayInputStream(bytes)));
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
    /*
    NOTE: For simplicity the queues will only get a fix maxByteSize which is dependent by their
    priority (minConnection-ratio) so now we're just doing the queue size (re-)adjustment for all queues once whenever a
    new RemoteFunctionClient get's created.
    However, this behavior will be optimized in the future.
     */
    //TODO: Improve the calculation with respect to the throughput and do the adjustments when necessary at run-time
    clientInstances.forEach(c -> c.queue.setMaxByteSize((long) (c.getPriority() * GLOBAL_MAX_QUEUE_BYTE_SIZE)));
  }

  public static long getGlobalUsedQueueMemory() {
    return clientInstances.stream().mapToLong(c -> c.queue.getByteSize()).sum();
  }

  public static double getGlobalThroughput() {
    return clientInstances.stream().mapToDouble(RemoteFunctionClient::getThroughput).sum();
  }

  public static double getGlobalArrivalRate() {
    return clientInstances.stream().mapToDouble(RemoteFunctionClient::getArrivalRate).sum();
  }

  public static long getGlobalMaxConnections() {
    return clientInstances.stream().mapToLong(RemoteFunctionClient::getMaxConnections).sum();
  }

  public static long getGlobalUsedConnections() {
    return clientInstances.stream().mapToLong(RemoteFunctionClient::getUsedConnections).sum();
  }

  public static Set<RemoteFunctionClient> getInstances() {
    return Collections.unmodifiableSet(clientInstances);
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

//  private void recalculatePerformance(long executionTime, TimeUnit timeUnit) {
//    recalculateSARET(executionTime, timeUnit);
//    recalculateRateOfService();
//    adjustQueueElementCount();
//  }

//  private void recalculateSARET(long executionTime, TimeUnit timeUnit) {
//    double executionTimeSeconds = (double) (timeUnit.toMicros(executionTime)) / 1_000_000d;
//    double requestRelevance = 1 / (rateOfService * REQUEST_RELEVANCE_FACTOR);
//    SARET = executionTimeSeconds * requestRelevance + SARET * (1d - requestRelevance);
//  }

//  private void recalculateRateOfService() {
//    rateOfService = getMaxConnections() / SARET;
//  }

  public double getRateOfService() {
    return rateOfService;
  }

  public int getMinConnections() {
    return connectorConfig == null ? 0 : connectorConfig.getMinConnectionsPerInstance();
  }

  public int getMaxConnections() {
    return connectorConfig == null ? 0 : connectorConfig.getMaxConnectionsPerInstance();
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

//  /**
//   * Sets the maximum feasible element count of the queue with respect to the {@link #REQUEST_TIMEOUT} and the {@link #rateOfService} of
//   * this RemoteFunctionClient.
//   */
//  private void adjustQueueElementCount() {
//    long maxFeasibleElements = (long) Math.ceil(rateOfService * REQUEST_TIMEOUT);
//    queue.setMaxSize(maxFeasibleElements);
//  }

  private void enqueue(final Marker marker, byte[] bytes, final Handler<AsyncResult<byte[]>> callback) {
    FunctionCall fc = new FunctionCall(marker, bytes, callback);

    /*if (Service.currentTimeMillis() > lastSizeAdjustment.get() + SIZE_ADJUSTMENT_INTERVAL
        && fc.getByteSize() + queue.getByteSize() > queue.getMaxByteSize()) {
      //Element won't fit into queue so we try to enlarge it
      adjustQueueByteSizes();
    }*/

    //In any case add the element to the queue
    queue.add(fc)
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
