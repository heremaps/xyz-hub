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

import com.google.common.io.ByteStreams;
import com.here.xyz.Payload;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.GZIPInputStream;
import org.apache.logging.log4j.Marker;

public abstract class RemoteFunctionClient {

  public static final long REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(Service.configuration.REMOTE_FUNCTION_REQUEST_TIMEOUT);
  private static int MEASUREMENT_INTERVAL = 1000; //1s

  protected Connector connectorConfig;
  protected RemoteFunctionConfig remoteFunction;

  private LongAdder requestsSinceLastArrivaleRateMeasurement = new LongAdder();
  private AtomicLong lastArrivalRateMeasurement = new AtomicLong(System.currentTimeMillis());

  private LongAdder responsesSinceLastThroughputMeasurement = new LongAdder();
  private AtomicLong lastThroughputMeasurement = new AtomicLong(System.currentTimeMillis());

  /**
   * The number of requests per second currently being executed by this RemoteFunctionClient.
   */
  private volatile double throughput;

  /**
   * The number of requests per second currently being submitted to this RemoteFunctionClient.
   */
  private volatile double arrivalRate;

  public RemoteFunctionClient(Connector connectorConfig) {
      this.connectorConfig = connectorConfig;
      remoteFunction = connectorConfig.remoteFunction;
  }

  public static RemoteFunctionClient getInstanceFor(Connector connectorConfig) {
    if (connectorConfig == null) {
      throw new NullPointerException("Can not create RemoteFunctionClient without connector configuration.");
    }
    if (connectorConfig.remoteFunction instanceof Connector.RemoteFunctionConfig.AWSLambda) {
      return new LambdaFunctionClient(connectorConfig);
    }
    else if (connectorConfig.remoteFunction instanceof Connector.RemoteFunctionConfig.Embedded) {
      return new EmbeddedFunctionClient(connectorConfig);
    }
    else if (connectorConfig.remoteFunction instanceof Http) {
      return new HTTPFunctionClient(connectorConfig);
    }
    else {
      throw new IllegalArgumentException("Unknown remote function type: " + connectorConfig.getClass().getSimpleName());
    }
  }

  protected static void checkResponseSize(byte[] response) throws HttpException {
    boolean isGZIP = response != null && response.length >= 2
        && GZIPInputStream.GZIP_MAGIC == (((int) response[0] & 0xff) | ((response[1] << 8) & 0xff00));

    assert response != null;
    if (isGZIP && response.length > Api.MAX_COMPRESSED_RESPONSE_LENGTH || response.length > Api.MAX_RESPONSE_LENGTH) {
      throw new HttpException(Api.RESPONSE_PAYLOAD_TOO_LARGE, Api.RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE);
    }
  }

  /**
   * Should be overridden in sub-classes to implement refreshing steps (e.g. creating client / connections) whenever the connector
   * configuration was changed during the runtime.
   */
  protected void onConnectorConfigUpdate() {}

    /**
     * Should be overridden in sub-classes to implement clean-up steps (e.g. closing client / connections).
     */
  void close() {}

  protected void submit(final Marker marker, byte[] bytes, final Handler<AsyncResult<byte[]>> callback) {
    invoke(marker, bytes, r -> {
      //This is the point where the request's response came back so measure the throughput
      invokeCompleted();
      callback.handle(r);
    });
    //This is the point where new requests arrive so measure the arrival time
    invokeStarted();
  }

  /**
   * Measures the occurance of events in relation to the time having passed by since the last measurement. This is at minimum the value of
   * {@link #MEASUREMENT_INTERVAL}.
   *
   * @param eventCount A counter for the events having occurred so far within the current time-interval
   * @param lastMeasurementTime The point in time when the last measurement was done This reference will be updated in case the
   * time-interval was exceeded
   * @return The new current value for the dimension. If the time-interval was exceeded this is a newly calculated value otherwise the
   * return value is -1.
   */
  protected final double measureDimension(LongAdder eventCount, AtomicLong lastMeasurementTime) {
    long now = System.currentTimeMillis();
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
    requestsSinceLastArrivaleRateMeasurement.increment();
    measureArrival();
  }

  private void measureArrival() {
    double newAR = measureDimension(requestsSinceLastArrivaleRateMeasurement, lastArrivalRateMeasurement);
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
   *
   * @param connectorConfig the connector configuration
   */
  final void setConnectorConfig(Connector connectorConfig) {
    if (!connectorConfig.id.equals(this.connectorConfig.id)) throw new IllegalArgumentException("Wrong connector config " +
        "was provided to an existing function client during a runtime update. IDs are not matching. " +
        "new ID: " + connectorConfig.id + " vs. old ID: " + this.connectorConfig.id);
    this.connectorConfig = connectorConfig;
    remoteFunction = connectorConfig.remoteFunction;
    onConnectorConfigUpdate();
  }

  /**
   *
   * @param currentValue The current value of the sliding average of the dimension
   * @param slideInValue The value to take into account for the new average additionally
   * @param slideInRelevance A number between 0 .. 1 indicating the relevance of the slideInValue in relation to the
   *  current value of the sliding average.
   * @return The new value of the sliding average
   */
  @SuppressWarnings("unused")
  protected double calculateSlidingAverage(double currentValue, double slideInValue, double slideInRelevance) {
    return currentValue * (1d - slideInRelevance) + slideInValue * slideInRelevance;
  }

  protected byte[] getDecompressed(final byte[] bytes) throws IOException {
    return ByteStreams.toByteArray(Payload.prepareInputStream(new ByteArrayInputStream(bytes)));
  }
}
