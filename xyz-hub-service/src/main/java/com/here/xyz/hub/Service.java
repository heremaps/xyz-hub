/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.hub;

import com.here.xyz.hub.cache.CacheClient;
import com.here.xyz.hub.cache.InMemoryCacheClient;
import com.here.xyz.hub.cache.MultiLevelCacheClient;
import com.here.xyz.hub.cache.RedisCacheClient;
import com.here.xyz.hub.cache.S3CacheClient;
import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.config.SettingsConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.config.SubscriptionConfigClient;
import com.here.xyz.hub.config.TagConfigClient;
import com.here.xyz.hub.connectors.ConfigUpdateThread;
import com.here.xyz.hub.connectors.WarmupRemoteFunctionThread;
import com.here.xyz.hub.rest.admin.MessageBroker;
import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.util.metrics.GcDurationMetric;
import com.here.xyz.hub.util.metrics.GlobalInflightRequestMemory;
import com.here.xyz.hub.util.metrics.GlobalUsedRfcConnections;
import com.here.xyz.hub.util.metrics.MajorGcCountMetric;
import com.here.xyz.hub.util.metrics.MemoryMetric;
import com.here.xyz.hub.util.metrics.base.CWBareValueMetricPublisher;
import com.here.xyz.hub.util.metrics.base.MetricPublisher;
import com.here.xyz.hub.util.metrics.net.ConnectionMetrics;
import com.here.xyz.hub.util.metrics.net.ConnectionMetrics.HubMetricsFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

public class Service extends Core {

  private static final Logger logger = LogManager.getLogger();


  public static final String XYZ_HUB_USER_AGENT = "XYZ-Hub/" + BUILD_VERSION;

  /**
   * The host ID.
   */
  public static final String HOST_ID = UUID.randomUUID().toString();

  /**
   * The shared map used across verticles
   */
  public static final String SHARED_DATA = "SHARED_DATA";

  /**
   * The key to access the global router in the shared data
   */
  public static final String GLOBAL_ROUTER = "GLOBAL_ROUTER";

  /**
   * The service configuration.
   */
  public static Config configuration;

  /**
   * The client to access the space configuration.
   */
  public static SpaceConfigClient spaceConfigClient;

  /**
   * The client to access the connector configuration.
   */
  public static ConnectorConfigClient connectorConfigClient;

  /**
   * The client to access the subscription configuration.
   */
  public static SubscriptionConfigClient subscriptionConfigClient;

  /**
   * The client to access the reader configuration.
   */
  public static TagConfigClient tagConfigClient;

  /**
   * The client to access runtime settings.
   */
  public static SettingsConfigClient settingsConfigClient;

  /**
   * A web client to access XYZ Hub nodes and other web resources.
   */
  public static WebClient webClient;

  /**
   * The cache client for the volatile service cache.
   */
  public static CacheClient volatileCacheClient;

  /**
   * The cache client for the static service cache.
   */
  public static CacheClient staticCacheClient;

  /**
   * The node's MessageBroker which is used to send AdminMessages.
   */
  public static MessageBroker messageBroker;

  /**
   * The hostname
   */
  private static String hostname;
  public static final boolean IS_USING_ZGC = isUsingZgc();

  private static final List<MetricPublisher> metricPublishers = new LinkedList<>();

  private static Router globalRouter;

  /**
   * The service entry point.
   */
  public static void main(String[] arguments) {
    Configurator.initialize("default", CONSOLE_LOG_CONFIG);
    isDebugModeActive = Arrays.asList(arguments).contains("--debug");

    VertxOptions vertxOptions = new VertxOptions()
        .setMetricsOptions(new MetricsOptions()
            .setEnabled(true)
            .setFactory(new HubMetricsFactory()));

    initializeVertx(vertxOptions)
        .compose(Service::initializeGlobalRouter)
        .compose(Core::initializeConfig)
        .compose(Core::initializeLogger)
        .compose(Service::parseConfiguration)
        .compose(Service::initializeClients)
        .compose(config -> Future.fromCompletionStage(ConfigUpdateThread.initialize()).map(config))
        .compose(config -> Future.fromCompletionStage(WarmupRemoteFunctionThread.initialize()).map(config))
        .compose(Service::initializeService)
        .onFailure(t -> logger.error("Service startup failed", t))
        .onSuccess(v -> logger.info("Service startup succeeded"));
  }

  private static Future<Vertx> initializeGlobalRouter(Vertx vertx) {
    globalRouter = Router.router(vertx);
    return Future.succeededFuture(vertx);
  }

  private static Future<JsonObject> parseConfiguration(JsonObject config) {
    configuration = config.mapTo(Config.class);
    configuration.defaultStorageIds = Stream.of(configuration.DEFAULT_STORAGE_ID.split(",")).map(String::trim).collect(Collectors.toList());

    return Future.succeededFuture(config);
  }

  private static Future<JsonObject> initializeClients(JsonObject config) {
    volatileCacheClient = new MultiLevelCacheClient(InMemoryCacheClient.getInstance(), RedisCacheClient.getInstance());
    staticCacheClient = new MultiLevelCacheClient(InMemoryCacheClient.getInstance(), S3CacheClient.getInstance());
    MessageBroker.getInstance().onSuccess(mb -> {
      messageBroker = mb;
      Node.initialize();
    });
    spaceConfigClient = SpaceConfigClient.getInstance();
    connectorConfigClient = ConnectorConfigClient.getInstance();
    subscriptionConfigClient = SubscriptionConfigClient.getInstance();
    tagConfigClient = TagConfigClient.getInstance();
    settingsConfigClient = SettingsConfigClient.getInstance();

    webClient = WebClient.create(vertx, new WebClientOptions()
        .setUserAgent(XYZ_HUB_USER_AGENT)
        .setTcpKeepAlive(configuration.HTTP_CLIENT_TCP_KEEPALIVE)
        .setIdleTimeout(configuration.HTTP_CLIENT_IDLE_TIMEOUT)
        .setTcpQuickAck(true)
        .setTcpFastOpen(true)
        .setPipelining(Service.configuration.HTTP_CLIENT_PIPELINING)
    );

    return settingsConfigClient.init()
        .compose(v -> settingsConfigClient.insertLocalSettings())
        .compose(v -> spaceConfigClient.init())
        .compose(v -> connectorConfigClient.init())
        .compose(v -> Future.fromCompletionStage(connectorConfigClient.insertLocalConnectors()))
        .compose(v -> subscriptionConfigClient.init())
        .compose(v -> tagConfigClient.init())
        .map(config)
        .onFailure(t -> logger.error("initializeClients failed", t))
        .onSuccess(v -> logger.info("initializeClients succeeded"));
  }

  private static Future<Void> initializeService(JsonObject config) {
    if (StringUtils.isEmpty(Service.configuration.VERTICLES_CLASS_NAMES)) {
      logger.error("At least one Verticle class name should be specified on VERTICLES_CLASS_NAMES. Service can't be started");
      return Future.failedFuture("service initialization failed");
    }

    final List<String> verticlesClassNames = Arrays.asList(Service.configuration.VERTICLES_CLASS_NAMES.split(","));
    int numInstances = Runtime.getRuntime().availableProcessors();
    final DeploymentOptions options = new DeploymentOptions()
        .setConfig(config)
        .setWorker(false)
        .setInstances(numInstances);

    final Promise<Void> sharedDataPromise = Promise.promise();
    final Future<Void> sharedDataFuture = sharedDataPromise.future();
    final Hashtable<String, Object> sharedData = new Hashtable<>() {{
      put(GLOBAL_ROUTER, globalRouter);
    }};

    sharedDataFuture.compose(r -> {
      final List<Future> futures = new ArrayList<>();

      verticlesClassNames.forEach(className -> {
        final Promise<AsyncResult<String>> deployVerticlePromise = Promise.promise();
        futures.add(deployVerticlePromise.future());

        logger.info("Deploying verticle: " + className);
        vertx.deployVerticle(className, options, deployVerticleHandler -> {
          if (deployVerticleHandler.failed()) {
            logger.warn("Unable to load verticle class:" + className, deployVerticleHandler.cause());
          }
          deployVerticlePromise.complete();
        });
      });

      return CompositeFuture.all(futures);
    }).onComplete(done -> {
      // at this point all verticles were initiated and all routers added as subrouter of globalRouter.
      vertx.eventBus().publish(SHARED_DATA, GLOBAL_ROUTER);

      logger.info("XYZ Hub " + BUILD_VERSION + " was started at " + new Date().toString());
      logger.info("Native transport enabled: " + vertx.isNativeTransportEnabled());
    });

    //Shared data initialization
    vertx.sharedData()
        .getAsyncMap(SHARED_DATA, asyncMapResult -> asyncMapResult.result().put(SHARED_DATA, sharedData, sharedDataPromise));

    Thread.setDefaultUncaughtExceptionHandler((thread, t) -> logger.error("Uncaught exception: ", t));

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      stopMetricPublishers();
      //This may fail, if we are OOM, but lets at least try.
      logger.warn("XYZ Service is going down at " + new Date().toString());
    }));

    startMetricPublishers();

    return sharedDataFuture;
  }

  public static String getHostname() {
    if (hostname == null) {
      final String hostname = Service.configuration != null ? Service.configuration.HOST_NAME : null;
      if (hostname != null && hostname.length() > 0) {
        Service.hostname = hostname;
      } else {
        try {
          Service.hostname = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
          logger.error("Unable to resolve the hostname using Java's API.", e);
          Service.hostname = "localhost";
        }
      }
    }
    return hostname;
  }

  public static long getUsedMemoryBytes() {
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  public static float getUsedMemoryPercent() {
    float used = getUsedMemoryBytes();
    float total = Runtime.getRuntime().totalMemory();
    return used / total * 100;
  }

  private static void startMetricPublishers() {
    if (configuration.PUBLISH_METRICS) {
      ConnectionMetrics.initialize();
      metricPublishers.add(new CWBareValueMetricPublisher(new MemoryMetric("JvmMemoryUtilization")));
      metricPublishers.add(new CWBareValueMetricPublisher(new MajorGcCountMetric("MajorGcCount")));
      metricPublishers.add(new CWBareValueMetricPublisher(new GcDurationMetric("GcDuration")));
      metricPublishers.add(new CWBareValueMetricPublisher(new GlobalUsedRfcConnections("GlobalUsedRfcConnections")));
      metricPublishers.add(new CWBareValueMetricPublisher(new GlobalInflightRequestMemory("GlobalInflightRequestMemory")));
      metricPublishers.addAll(ConnectionMetrics.startConnectionMetricPublishers());
    }
  }

  private static boolean isUsingZgc() {
    List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
      if (gcMxBean.getName().startsWith("ZGC")) {
        logger.info("Service is using ZGC.");
        return true;
      }
    }
    return false;
  }

  private static void stopMetricPublishers() {
    metricPublishers.forEach(MetricPublisher::stop);
  }

  public static int getPublicPort() {
    if (configuration.XYZ_HUB_PUBLIC_ENDPOINT == null) return configuration.HTTP_PORT;
    try {
      URI endpoint = new URI(configuration.XYZ_HUB_PUBLIC_ENDPOINT);
      int port = endpoint.getPort();
      return port > 0 ? port : 80;
    }
    catch (URISyntaxException e) {
      return configuration.HTTP_PORT;
    }
  }

  /**
   * @return The "environment ID" of this service deployment.
   */
  public static String getEnvironmentIdentifier() {
    if (configuration.ENVIRONMENT_NAME == null) return "default";
    if (configuration.AWS_REGION != null) return configuration.ENVIRONMENT_NAME + "_" + configuration.AWS_REGION;
    return configuration.ENVIRONMENT_NAME;
  }
}
