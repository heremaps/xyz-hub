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

package com.here.xyz.hub;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.cache.CacheClient;
import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.config.SubscriptionConfigClient;
import com.here.xyz.hub.connectors.BurstAndUpdateThread;
import com.here.xyz.hub.connectors.WarmupRemoteFunctionThread;
import com.here.xyz.hub.rest.admin.MessageBroker;
import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import com.here.xyz.hub.util.ARN;
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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
   * A web client to access XYZ Hub nodes and other web resources.
   */
  public static WebClient webClient;

  /**
   * The cache client for the service.
   */
  public static CacheClient cacheClient;

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

  private static final String CONFIG_FILE = "config.json";

  /**
   * The service entry point.
   */
  public static void main(String[] arguments) {
    boolean debug = Arrays.asList(arguments).contains("--debug");
    VertxOptions vertxOptions = new VertxOptions()
        .setMetricsOptions(new MetricsOptions()
          .setEnabled(true)
          .setFactory(new HubMetricsFactory()));
    initialize(vertxOptions, debug, CONFIG_FILE, Service::onConfigLoaded);
  }

  /**
   *
   */
  private static void onConfigLoaded(JsonObject jsonConfig) {
    configuration = jsonConfig.mapTo(Config.class);

    cacheClient = CacheClient.getInstance();
    MessageBroker.getInstance().onSuccess(mb -> {
      messageBroker = mb;
      Node.initialize();
    });
    spaceConfigClient = SpaceConfigClient.getInstance();
    connectorConfigClient = ConnectorConfigClient.getInstance();
    subscriptionConfigClient = SubscriptionConfigClient.getInstance();

    webClient = WebClient.create(vertx, new WebClientOptions()
        .setUserAgent(XYZ_HUB_USER_AGENT)
        .setTcpKeepAlive(configuration.HTTP_CLIENT_TCP_KEEPALIVE)
        .setIdleTimeout(configuration.HTTP_CLIENT_IDLE_TIMEOUT)
        .setTcpQuickAck(true)
        .setTcpFastOpen(true)
        .setPipelining(Service.configuration.HTTP_CLIENT_PIPELINING)
    );

    globalRouter = Router.router(vertx);

    spaceConfigClient.init(spaceConfigReady -> {
      if (spaceConfigReady.succeeded()) {
        connectorConfigClient.init(connectorConfigReady -> {
          if (connectorConfigReady.succeeded()) {
            if (Service.configuration.INSERT_LOCAL_CONNECTORS) {
              connectorConfigClient.insertLocalConnectors(result -> onLocalConnectorsInserted(result, jsonConfig));
            } else {
              onLocalConnectorsInserted(Future.succeededFuture(), jsonConfig);
            }
          }
        });
        subscriptionConfigClient.init(subscriptionConfigReady -> {});
      }
    });
  }

  private static void onLocalConnectorsInserted(AsyncResult<Void> result, JsonObject config) {
    if (result.failed()) {
      logger.error("Failed to insert local connectors.", result.cause());
    } else {
      BurstAndUpdateThread.initialize(initializeAr -> onBustAndUpdateThreadStarted(initializeAr, config));
    }
  }

  private static void onBustAndUpdateThreadStarted(AsyncResult<Void> result, JsonObject config) {
    if (result.failed()) {
      logger.error("Failed to start BurstAndUpdateThread.", result.cause());
    } else {
      // start warmup thread after connectors have been checked by BurstAndUpdateThread
      WarmupRemoteFunctionThread.initialize(initializeAr -> onServiceInitialized(initializeAr, config));
    }
  }

  private static void onServiceInitialized(AsyncResult<Void> result, JsonObject config) {
    if (result.failed()) {
      logger.error("Failed to initialize Connectors. Service can't be started.", result.cause());
      return;
    }

    if (StringUtils.isEmpty(Service.configuration.VERTICLES_CLASS_NAMES)) {
      logger.error("At least one Verticle class name should be specified on VERTICLES_CLASS_NAMES. Service can't be started");
      return;
    }

    final List<String> verticlesClassNames = Arrays.asList(Service.configuration.VERTICLES_CLASS_NAMES.split(","));
    int numInstances = Runtime.getRuntime().availableProcessors() * 2 / verticlesClassNames.size();
    final DeploymentOptions options = new DeploymentOptions()
        .setConfig(config)
        .setWorker(false)
        .setInstances(numInstances);

    final Promise<Void> sharedDataPromise = Promise.promise();
    final Future<Void> sharedDataFuture = sharedDataPromise.future();
    final Hashtable<String, Object> sharedData = new Hashtable<String, Object>() {{
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
            logger.warn("Unable to load verticle class:" + className);
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

  /**
   * The service configuration.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Config {

    /**
     * The global maximum number of http client connections.
     */
    public int MAX_GLOBAL_HTTP_CLIENT_CONNECTIONS;

    /**
     * Size of the off-heap cache in megabytes.
     */
    public int OFF_HEAP_CACHE_SIZE_MB;

    /**
     * The port of the HTTP server.
     */
    public int HTTP_PORT;

    /**
     * The hostname, which under instances can use to contact the this service node.
     */
    public String HOST_NAME;

    /**
     * The initial number of instances.
     */
    public int INSTANCE_COUNT;

    /**
     * The S3 Bucket which could be used by connectors with transfer limitations to relocate responses.
     */
    public String XYZ_HUB_S3_BUCKET;

    /**
     * The public endpoint.
     */
    public String XYZ_HUB_PUBLIC_ENDPOINT;

    /**
     * The public health-check endpoint, i.e. /hub/
     */
    public String XYZ_HUB_PUBLIC_HEALTH_ENDPOINT = "/hub/";

    /**
     * The redis host.
     */
    @Deprecated
    public String XYZ_HUB_REDIS_HOST;

    /**
     * The redis port.
     */
    @Deprecated
    public int XYZ_HUB_REDIS_PORT;

    /**
     * The redis connection string.
     */
    public String XYZ_HUB_REDIS_URI;

    /**
     * The redis auth token.
     */
    public String XYZ_HUB_REDIS_AUTH_TOKEN;

    /**
     * Adds backward-compatibility for the deprecated environment variables XYZ_HUB_REDIS_HOST & XYZ_HUB_REDIS_PORT.
     * @return
     */
    //TODO: Remove this workaround after the deprecation period
    @JsonIgnore
    public String getRedisUri() {
      if (XYZ_HUB_REDIS_HOST != null) {
        String protocol = XYZ_HUB_REDIS_AUTH_TOKEN != null ? "rediss" : "redis";
        int port = XYZ_HUB_REDIS_PORT != 0 ? XYZ_HUB_REDIS_PORT : 6379;
        return protocol + "://" + XYZ_HUB_REDIS_HOST + ":" + port;
      }
      else
        return XYZ_HUB_REDIS_URI;
    }

    /**
     * The urls of remote hub services, separated by semicolon ';'
     */
    public String XYZ_HUB_REMOTE_SERVICE_URLS;
    private List<String> hubRemoteServiceUrls;

    public List<String> getHubRemoteServiceUrls() {
      if (hubRemoteServiceUrls == null)
        hubRemoteServiceUrls = XYZ_HUB_REMOTE_SERVICE_URLS == null ? null : Arrays.asList(XYZ_HUB_REMOTE_SERVICE_URLS.split(";"));
      return hubRemoteServiceUrls;
    }

    /**
     * The authorization type.
     */
    public Authorization.AuthorizationType XYZ_HUB_AUTH;

    /**
     * The public key used for verifying the signature of the JWT tokens.
     */
    public String JWT_PUB_KEY;

    /**
     * Adds backward-compatibility for public keys without header & footer.
     * @return
     */
    //TODO: Remove this workaround after the deprecation period
    @JsonIgnore
    public String getJwtPubKey() {
      String jwtPubKey = JWT_PUB_KEY;
      if (jwtPubKey != null) {
        if (!jwtPubKey.startsWith("-----"))
          jwtPubKey = "-----BEGIN PUBLIC KEY-----\n" + jwtPubKey;
        if (!jwtPubKey.endsWith("-----"))
          jwtPubKey = jwtPubKey + "\n-----END PUBLIC KEY-----";
      }
      return jwtPubKey;
    }

    /**
     * If set to true, the connectors configuration will be populated with connectors defined in connectors.json.
     */
    public boolean INSERT_LOCAL_CONNECTORS;

    /**
     * If set to true, the connectors will receive health checks. Further an unhealthy connector gets deactivated
     * automatically if the connector config does not include skipAutoDisable.
     */
    public boolean ENABLE_CONNECTOR_HEALTH_CHECKS;

    /**
     * If true HERE Tiles are get handled as Base 4 encoded. Default is false (Base 10).
     */
    public boolean USE_BASE_4_H_TILES;

    /**
     * The ID of the default storage connector.
     */
    public String DEFAULT_STORAGE_ID;

    /**
     * The PostgreSQL URL.
     */
    public String STORAGE_DB_URL;

    /**
     * The database user.
     */
    public String STORAGE_DB_USER;

    /**
     * The database password.
     */
    public String STORAGE_DB_PASSWORD;

    /**
     * The http connector host.
     */
    public String PSQL_HTTP_CONNECTOR_HOST;

    /**
     * The ARN of the space table in DynamoDB.
     */
    public String SPACES_DYNAMODB_TABLE_ARN;

    /**
     * The ARN of the connectors table in DynamoDB.
     */
    public String CONNECTORS_DYNAMODB_TABLE_ARN;

    /**
     * The ARN of the packages table in DynamoDB.
     */
    public String PACKAGES_DYNAMODB_TABLE_ARN;

    /**
     * The ARN of the subscriptions table in DynamoDB.
     */
    public String SUBSCRIPTIONS_DYNAMODB_TABLE_ARN;

    /**
     * The ARN of the admin message topic.
     */
    public ARN ADMIN_MESSAGE_TOPIC_ARN;

    /**
     * The JWT token used for sending admin messages.
     */
    public String ADMIN_MESSAGE_JWT;

    /**
     * The port for the admin message server.
     */
    public int ADMIN_MESSAGE_PORT;

    /**
     * The total size assigned for remote functions queues.
     */
    public int GLOBAL_MAX_QUEUE_SIZE; //MB

    /**
     * The default timeout for remote function requests in seconds.
     */
    public int REMOTE_FUNCTION_REQUEST_TIMEOUT; //seconds

    /**
     * OPTIONAL: The maximum timeout for remote function requests in seconds.
     * If not specified, the value of {@link #REMOTE_FUNCTION_REQUEST_TIMEOUT} will be used.
     */
    public int REMOTE_FUNCTION_MAX_REQUEST_TIMEOUT; //seconds

    /**
     * @return the value of {@link #REMOTE_FUNCTION_MAX_REQUEST_TIMEOUT} if specified.
     *  The value of {@link #REMOTE_FUNCTION_REQUEST_TIMEOUT} otherwise.
     */
    @JsonIgnore
    public int getRemoteFunctionMaxRequestTimeout() {
      return REMOTE_FUNCTION_MAX_REQUEST_TIMEOUT > 0 ? REMOTE_FUNCTION_MAX_REQUEST_TIMEOUT : REMOTE_FUNCTION_REQUEST_TIMEOUT;
    }

    /**
     * The maximum amount of RemoteFunction connections to be opened by this node.
     */
    public int REMOTE_FUNCTION_MAX_CONNECTIONS;

    /**
     * The amount of memory (in MB) which can be taken by incoming requests.
     */
    public int GLOBAL_INFLIGHT_REQUEST_MEMORY_SIZE_MB;

    /**
     * A value between 0 and 1 defining a threshold as percentage of utilized RemoteFunction max-connections after which to start
     * prioritizing more important connectors over less important ones.
     *
     * @see Config#REMOTE_FUNCTION_MAX_CONNECTIONS
     */
    public float REMOTE_FUNCTION_CONNECTION_HIGH_UTILIZATION_THRESHOLD;

    /**
     * A value between 0 and 1 defining a threshold as percentage of utilized service memory for in-flight request after which to start
     * prioritizing more important connectors over less important ones.
     */
    public float GLOBAL_INFLIGHT_REQUEST_MEMORY_HIGH_UTILIZATION_THRESHOLD;

    /**
     * A value between 0 and 1 defining a threshold as percentage of utilized service memory which depicts a very high utilization of the
     * the memory. The service uses that threshold to perform countermeasures to protect the service from overload.
     */
    public float SERVICE_MEMORY_HIGH_UTILIZATION_THRESHOLD;

    /**
     * The remote function pool ID to be used to select the according remote functions for this Service environment.
     */
    public String REMOTE_FUNCTION_POOL_ID;

    /**
     * The web root for serving static resources from the file system.
     */
    public String FS_WEB_ROOT;

    /**
     * The name of the upload limit header
     */
    public String UPLOAD_LIMIT_HEADER_NAME;

    /**
     * The code which gets returned if UPLOAD_LIMIT is reached
     */
    public int UPLOAD_LIMIT_REACHED_HTTP_CODE;

    /**
     * The message which gets returned if UPLOAD_LIMIT is reached
     */
    public String UPLOAD_LIMIT_REACHED_MESSAGE;

    /**
     * The name of the health check header to instruct for additional health status information.
     */
    public String HEALTH_CHECK_HEADER_NAME;

    /**
     * The value of the health check header to instruct for additional health status information.
     */
    public String HEALTH_CHECK_HEADER_VALUE;

    /**
     * An identifier for the service environment.
     */
    public String ENVIRONMENT_NAME;

    /**
     * Whether to publish custom service metrics like JVM memory utilization or Major GC count.
     */
    public boolean PUBLISH_METRICS;

    /**
     * The AWS region this service is running in. Value is <code>null</code> if not running in AWS.
     */
    public String AWS_REGION;

    /**
     * The verticles class names to be deployed, separated by comma
     */
    public String VERTICLES_CLASS_NAMES;

    /**
     * The default ECPS phrase. (Mainly for testing purposes)
     */
    public String DEFAULT_ECPS_PHRASE;

    /**
     * The topic ARN for Space modification notifications. If no value is provided no notifications will be sent.
     */
    public String MSE_NOTIFICATION_TOPIC;

    /**
     * The maximum size of an event transiting between connector -> service -> client.
     * Validation is only applied when MAX_SERVICE_RESPONSE_SIZE is bigger than zero.
     * @deprecated Use instead MAX_UNCOMPRESSED_RESPONSE_SIZE
     */
    public int MAX_SERVICE_RESPONSE_SIZE;

    /**
     * The maximum uncompressed request size in bytes supported on API calls.
     * If uncompressed request size is bigger than MAX_UNCOMPRESSED_REQUEST_SIZE, an error with status code 413 will be sent.
     */
    public long MAX_UNCOMPRESSED_REQUEST_SIZE;

    /**
     * The maximum uncompressed response size in bytes supported on API calls.
     * If uncompressed response size is bigger than MAX_UNCOMPRESSED_RESPONSE_SIZE, an error with status code 513 will be sent.
     */
    public long MAX_UNCOMPRESSED_RESPONSE_SIZE;


    /**
     * The maximum http response size in bytes supported on API calls.
     * If response size is bigger than MAX_HTTP_RESPONSE_SIZE, an error with status code 513 will be sent.
     * Validation is only applied when MAX_HTTP_RESPONSE_SIZE is bigger than zero.
     * @deprecated Use instead MAX_UNCOMPRESSED_RESPONSE_SIZE
     */
    public int MAX_HTTP_RESPONSE_SIZE;

    /**
     * Whether to activate pipelining for the HTTP client of the service.
     */
    public boolean HTTP_CLIENT_PIPELINING;

    /**
     * Whether to activate TCP keepalive for the HTTP client of the service.
     */
    public boolean HTTP_CLIENT_TCP_KEEPALIVE = true;

    /**
     * The idle connection timeout in seconds for the HTTP client of the service.
     * Setting it to 0 will make the connections not timing out at all.
     */
    public int HTTP_CLIENT_IDLE_TIMEOUT = 120;

    /**
     * List of fields, separated by comma, which are optional on feature's namespace property.
     */
    public List<String> FEATURE_NAMESPACE_OPTIONAL_FIELDS = Collections.emptyList();
    private Map<String, Object> FEATURE_NAMESPACE_OPTIONAL_FIELDS_MAP;

    /**
     * When set, modifies the Stream-Info header name to the value specified.
     */
    public String CUSTOM_STREAM_INFO_HEADER_NAME;

    /**
     * Whether the service should use InstanceProviderCredentialsProfile with cached credential when utilizing AWS clients.
     */
    public boolean USE_AWS_INSTANCE_CREDENTIALS_WITH_REFRESH;

    public boolean containsFeatureNamespaceOptionalField(String field) {
      if (FEATURE_NAMESPACE_OPTIONAL_FIELDS_MAP == null) {
        FEATURE_NAMESPACE_OPTIONAL_FIELDS_MAP = new HashMap<String,Object>() {{
          FEATURE_NAMESPACE_OPTIONAL_FIELDS.forEach(k -> put(k, null));
        }};
      }

      return FEATURE_NAMESPACE_OPTIONAL_FIELDS_MAP.containsKey(field);
    }

    /**
     * Flag that indicates whether the creation of features on history spaces contains features in the request payload with uuid.
     */
    public boolean MONITOR_FEATURES_WITH_UUID = true;
  }

  /**
   * That message can be used to change the log-level of one or more service-nodes. The specified level must be a valid log-level. As this
   * is a {@link RelayedMessage} it can be sent to a specific service-node or to all service-nodes regardless of the first service node by
   * which it was received.
   *
   * Specifying the property {@link RelayedMessage#relay} to true will relay the message to the specified destination. If no destination is
   * specified the message will be relayed to all service-nodes (broadcast).
   */
  @SuppressWarnings("unused")
  static class ChangeLogLevelMessage extends RelayedMessage {

    private String level;

    public String getLevel() {
      return level;
    }

    public void setLevel(String level) {
      this.level = level;
    }

    @Override
    protected void handleAtDestination() {
      logger.info("LOG LEVEL UPDATE requested. New level will be: " + level);

      Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.getLevel(level));

      logger.info("LOG LEVEL UPDATE performed. New level is now: " + level);
    }
  }
}
