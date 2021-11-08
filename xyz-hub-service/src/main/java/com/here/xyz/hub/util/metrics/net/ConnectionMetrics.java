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

package com.here.xyz.hub.util.metrics.net;

import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static com.here.xyz.hub.util.metrics.base.Metric.MetricUnit.BYTES;
import static com.here.xyz.hub.util.metrics.base.Metric.MetricUnit.MILLISECONDS;

import com.here.xyz.hub.Core;
import com.here.xyz.hub.connectors.HTTPFunctionClient.HttpFunctionRegistry;
import com.here.xyz.hub.util.metrics.base.AggregatingMetric;
import com.here.xyz.hub.util.metrics.base.AggregatingMetric.AggregatedValues;
import com.here.xyz.hub.util.metrics.base.AttributedMetricCollection;
import com.here.xyz.hub.util.metrics.base.AttributedMetricCollection.Attribute;
import com.here.xyz.hub.util.metrics.base.CWAggregatedValuesPublisher;
import com.here.xyz.hub.util.metrics.base.CWAttributedMetricCollectionPublisher;
import com.here.xyz.hub.util.metrics.base.Metric.MetricUnit;
import com.here.xyz.hub.util.metrics.base.MetricPublisher;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.metrics.ClientMetrics;
import io.vertx.core.spi.metrics.HttpClientMetrics;
import io.vertx.core.spi.metrics.TCPMetrics;
import io.vertx.core.spi.observability.HttpRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

public class ConnectionMetrics {

  static final String TARGET = "target";

  public static AggregatingMetric httpClientQueueingTime;
  public static AttributedMetricCollection<AggregatedValues> tcpReadBytes;
  public static String TCP_READ_BYTES_METRIC_NAME = "TcpReadBytes";
  public static AttributedMetricCollection<AggregatedValues> tcpWrittenBytes;
  public static String TCP_WRITTEN_BYTES_METRIC_NAME = "TcpWrittenBytes";
  public static AttributedMetricCollection<AggregatedValues> httpRequestLatency;
  public static String HTTP_REQUEST_LATENCY_METRIC_NAME = "HttpRequestLatency";

  private static final String getTargetByUrl(String url) {
    String connectorId = HttpFunctionRegistry.getConnectorIdByUrl(url);
    if (connectorId != null)
      return HttpFunctionRegistry.isMetricsActive(connectorId) ? connectorId : null;
    else
      return url;
  }

  private static final String getTargetByHostAndPort(String hostname, int port) {
    String connectorId = HttpFunctionRegistry.getConnectorIdByHostAndPort(hostname, port);
    if (connectorId != null)
      return HttpFunctionRegistry.isMetricsActive(connectorId) ? connectorId : null;
    else
      return hostname + ":" + port;
  }

  private static void aggregate(String metricName, String target, MetricUnit unit, Map<String, AggregatingMetric> metricsMap,
      AttributedMetricCollection<AggregatedValues> metricCollection, double valueToAggregate) {
    if (metricCollection == null) return;
    metricsMap.computeIfAbsent(target, t -> {
      AggregatingMetric m = new AggregatingMetric(metricName, unit);
      tcpReadBytes.addMetric(m, new Attribute(TARGET, target));
      return m;
    }).addValue(valueToAggregate);
  }

  public static Collection<MetricPublisher> startConnectionMetricPublishers() {
    List<MetricPublisher> publishers = new ArrayList<>();
    //HTTP client queueing time
    publishers.add(new CWAggregatedValuesPublisher(
        httpClientQueueingTime = new AggregatingMetric("HttpClientQueueingTime", MILLISECONDS)));

    //Open outbound TCP connections by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(new CurrentTcpConnections()));

    //New outbound TCP connections by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(new NewTcpConnections()));

    //TCP connection errors by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(new TcpErrors()));

    //Read bytes through TCP connections by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(
        tcpReadBytes = new AttributedMetricCollection(TCP_READ_BYTES_METRIC_NAME, BYTES)));

    //Written bytes through TCP connections by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(
        tcpWrittenBytes = new AttributedMetricCollection(TCP_WRITTEN_BYTES_METRIC_NAME, BYTES)));

    //Reset HTTP requests count by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(new ResetHttpRequests()));

    //HTTP request latency by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(
        httpRequestLatency = new AttributedMetricCollection(HTTP_REQUEST_LATENCY_METRIC_NAME, MILLISECONDS)));

    //New HTTP requests by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(new HttpRequests()));
    
    //Inflight HTTP requests by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(new HttpRequestsInflight()));

    //Current HTTP connection pool utilization by remote target
    publishers.add(new CWAttributedMetricCollectionPublisher(new HttpPoolUtilization()));
    
    return publishers;
  }

  public static class HubTCPMetrics implements TCPMetrics {

    public static final Map<String, LongAdder> currentConnections = new ConcurrentHashMap<>();
    public static final Map<String, LongAdder> newConnections = new ConcurrentHashMap<>();
    public static final Map<String, LongAdder> exceptionCount = new ConcurrentHashMap<>();
    private static final Map<String, AggregatingMetric> readBytesMetrics = new ConcurrentHashMap<>();
    private static final Map<String, AggregatingMetric> writtenBytesMetrics = new ConcurrentHashMap<>();

    private static class SocketMetric {
      SocketAddress remoteAddress;
      String remoteName;
      String target;
    }

    @Override
    public Object connected(SocketAddress remoteAddress, String remoteName) {
      String target = getTargetByHostAndPort(remoteAddress.hostName(), remoteAddress.port());
      if (target != null) {
        SocketMetric sm = new SocketMetric();
        sm.remoteAddress = remoteAddress;
        sm.remoteName = remoteName;
        sm.target = target;
        currentConnections.computeIfAbsent(sm.target, t -> new LongAdder()).increment();
        newConnections.computeIfAbsent(sm.target, t -> new LongAdder()).increment();
        //System.out.println("######### TCP-METRICS (" + sm.target + "): NewTcpConnection");
        return sm;
      }
      return null;
    }

    @Override
    public void disconnected(Object socketMetric, SocketAddress remoteAddress) {
      if (socketMetric == null) return;
      LongAdder adder = currentConnections.get(((SocketMetric) socketMetric).target);
      if (adder != null)
        adder.decrement();
    }

    @Override
    public void bytesRead(Object socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
      if (socketMetric == null) return;
      //System.out.println("######### TCP-METRICS (" + ((SocketMetric) socketMetric).target + "): bytesRead: " + numberOfBytes);
      aggregate(TCP_READ_BYTES_METRIC_NAME, ((SocketMetric) socketMetric).target, BYTES, readBytesMetrics, tcpReadBytes, numberOfBytes);
    }

    @Override
    public void bytesWritten(Object socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
      if (socketMetric == null) return;
      //System.out.println("######### TCP-METRICS (" + ((SocketMetric) socketMetric).target + "): bytesWritten: " + numberOfBytes);
      aggregate(TCP_WRITTEN_BYTES_METRIC_NAME, ((SocketMetric) socketMetric).target, BYTES, writtenBytesMetrics, tcpWrittenBytes, numberOfBytes);
    }

    @Override
    public void exceptionOccurred(Object socketMetric, SocketAddress remoteAddress, Throwable t) {
      if (socketMetric == null) return;
      exceptionCount.computeIfAbsent(((SocketMetric) socketMetric).target, target -> new LongAdder()).increment();
    }
  }

  static class HubClientMetrics implements ClientMetrics {

    public SocketAddress remoteAddress;
    public int maxPoolSize;
    public String target;

    private static final Map<String, AggregatingMetric> httpRequestLatencyMetrics = new ConcurrentHashMap<>();
    public static final Map<String, LongAdder> resetHttpRequests = new ConcurrentHashMap<>();
    public static final Map<String, LongAdder> httpRequestsInflight = new ConcurrentHashMap<>();
    public static final Map<String, LongAdder> httpRequests = new ConcurrentHashMap<>();

    HubClientMetrics(SocketAddress remoteAddress, int maxPoolSize, String target) {
      this.remoteAddress = remoteAddress;
      this.maxPoolSize = maxPoolSize;
      this.target = target;
    }

    private static final Logger logger = LogManager.getLogger();

    static class RequestMetric {
      public HttpRequest request;
      private String target;
      private Marker marker;
      private long requestStart = Core.currentTimeMillis();
      public Marker getMarker() {
        if (marker == null)
          marker = new Log4jMarker(request.headers().get(STREAM_ID));
        return marker;
      }
    }

    @Override
    public Object enqueueRequest() {
      return Core.currentTimeMillis();
    }

    @Override
    public void dequeueRequest(Object taskMetric) {
      if (httpClientQueueingTime == null) return;
      long queueingTime = Core.currentTimeMillis() - (long) taskMetric;
      //System.out.println("############## Request handling took: " + queueingTime);
      httpClientQueueingTime.addValue(queueingTime);
    }

    @Override
    public Object requestBegin(String uri, Object request) {
      String target = getTargetByUrl(((HttpRequest) request).absoluteURI());
      if (request instanceof HttpRequest && target != null) {
        RequestMetric rm = new RequestMetric();
        rm.request = (HttpRequest) request;
        rm.target = target;
        httpRequestsInflight.computeIfAbsent(rm.target, t -> new LongAdder()).increment();
        httpRequests.computeIfAbsent(rm.target, t -> new LongAdder()).increment();
        return rm;
      }
      return null;
    }

    @Override
    public void requestReset(Object requestMetric) {
      if (requestMetric == null) return;
      resetHttpRequests.computeIfAbsent(((RequestMetric) requestMetric).target, target -> new LongAdder()).increment();
      removeInflight((RequestMetric) requestMetric);
    }

    @Override
    public void requestEnd(Object requestMetric, long bytesWritten) {
      if (requestMetric == null) return;
      //System.out.println("######### METRICS (" + ((RequestMetric) requestMetric).target + "): requestEnd, bytesWritten: " + bytesWritten);
    }

    @Override
    public void responseEnd(Object requestMetric, long bytesRead) {
      if (requestMetric == null) return;
      //System.out.println("######### METRICS(" + ((RequestMetric) requestMetric).target + "): responseEnd, bytesRead: " + bytesRead);
      aggregate(HTTP_REQUEST_LATENCY_METRIC_NAME, ((RequestMetric) requestMetric).target, MILLISECONDS, httpRequestLatencyMetrics, httpRequestLatency, Core.currentTimeMillis() - ((RequestMetric) requestMetric).requestStart);
      removeInflight((RequestMetric) requestMetric);
    }

    private void removeInflight(RequestMetric requestMetric) {
      if (requestMetric == null) return;
      LongAdder adder = httpRequestsInflight.get(requestMetric.target);
      if (adder != null)
        adder.decrement();
    }
  }

  public static class HubHttpClientMetrics implements HttpClientMetrics {

    TCPMetrics tcpDelegate = new HubTCPMetrics();
    public static final Map<String, LongAdder> endpointsConnected = new ConcurrentHashMap<>();

    @Override
    public ClientMetrics createEndpointMetrics(SocketAddress remoteAddress, int maxPoolSize) {
      String target = getTargetByHostAndPort(remoteAddress.hostName(), remoteAddress.port());
      if (target == null) return null;
      return new HubClientMetrics(remoteAddress, maxPoolSize, target);
    }

    @Override
    public void endpointConnected(ClientMetrics endpointMetric) {
      if (endpointMetric == null) return;
      endpointsConnected.computeIfAbsent(((HubClientMetrics) endpointMetric).target, t -> new LongAdder()).increment();
      //System.out.println("############ ENDPOINT CONNECTED (" + ((HubClientMetrics) endpointMetric).target + "): " + ((HubClientMetrics) endpointMetric).remoteAddress + ", maxPoolSize: " + ((HubClientMetrics) endpointMetric).maxPoolSize);
    }

    @Override
    public void endpointDisconnected(ClientMetrics endpointMetric) {
      if (endpointMetric == null) return;
      LongAdder adder = endpointsConnected.get(((HubClientMetrics) endpointMetric).target);
      if (adder != null)
        adder.decrement();
      //System.out.println("############ ENDPOINT DISCONNECTED (" + ((HubClientMetrics) endpointMetric).target + "): " + ((HubClientMetrics) endpointMetric).remoteAddress + ", maxPoolSize: " + ((HubClientMetrics) endpointMetric).maxPoolSize);
    }

    @Override
    public Object connected(SocketAddress remoteAddress, String remoteName) {
      return tcpDelegate.connected(remoteAddress, remoteName);
    }

    @Override
    public void disconnected(Object socketMetric, SocketAddress remoteAddress) {
      tcpDelegate.disconnected(socketMetric, remoteAddress);
    }

    @Override
    public void bytesRead(Object socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
      tcpDelegate.bytesRead(socketMetric, remoteAddress, numberOfBytes);
    }

    @Override
    public void bytesWritten(Object socketMetric, SocketAddress remoteAddress, long numberOfBytes) {
      tcpDelegate.bytesWritten(socketMetric, remoteAddress, numberOfBytes);
    }

    @Override
    public void exceptionOccurred(Object socketMetric, SocketAddress remoteAddress, Throwable t) {
      tcpDelegate.exceptionOccurred(socketMetric, remoteAddress, t);
    }
  }
}
