/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.cache.CacheClient;
import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.connectors.BurstAndUpdateThread;
import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import com.here.xyz.hub.util.ARN;
import com.here.xyz.hub.util.ConfigDecryptor;
import com.here.xyz.hub.util.ConfigDecryptor.CryptoException;
import com.here.xyz.hub.util.metrics.CloudWatchMetricPublisher;
import com.here.xyz.hub.util.metrics.MajorGcCountMetric;
import com.here.xyz.hub.util.metrics.MemoryMetric;
import com.here.xyz.hub.util.metrics.Metric;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.util.CachedClock;
import org.apache.logging.log4j.core.util.NetUtils;

public class Service {

  private static final Logger logger = LogManager.getLogger();
  private static final CachedClock clock = CachedClock.instance();

  /**
   * The service start time.
   */
  public static final long START_TIME = Service.currentTimeMillis();

  /**
   * The build version.
   */
  public static final String BUILD_VERSION = getBuildProperty("xyzhub.version");
  public static final String XYZ_HUB_USER_AGENT = "XYZ-Hub/" + Service.BUILD_VERSION;

  /**
   * The build time.
   */
  public static final long BUILD_TIME = getBuildTime();

  /**
   * The host ID.
   */
  public static final String HOST_ID = UUID.randomUUID().toString();

  /**
   * The shared map used across verticles
   */
  public static final String SHARED_DATA = "SHARED_DATA";

  /**
   * The key to access a router list in the shared data
   */
  public static final String ROUTERS = "ROUTERS";

  /**
   * The LOG4J configuration file.
   */
  private static final String CONSOLE_LOG_CONFIG = "log4j2-console-plain.json";

  /**
   * The Vertx worker pool size environment variable.
   */
  private static final String VERTX_WORKER_POOL_SIZE = "VERTX_WORKER_POOL_SIZE";

  /**
   * The http server options to be initialized
   */
  private static final HttpServerOptions SERVER_OPTIONS = new HttpServerOptions()
      .setCompressionSupported(true)
      .setDecompressionSupported(true)
      .setHandle100ContinueAutomatically(true)
      .setTcpQuickAck(true)
      .setTcpFastOpen(true)
      .setMaxInitialLineLength(16 * 1024)
      .setIdleTimeout(300);

  /**
   * The entry point to the Vert.x core API.
   */
  public static Vertx vertx;

  /**
   * The service configuration.
   */
  public static Config configuration;

  /**
   * The client to access the space configuration.
   */
  public static SpaceConfigClient spaceConfigClient;

  /**
   * The client to access the the connector configuration.
   */
  public static ConnectorConfigClient connectorConfigClient;

  /**
   * A web client to access XYZ Hub nodes and other web resources.
   */
  public static WebClient webClient;

  /**
   * The cache client for the service.
   */
  public static CacheClient cacheClient;

  /**
   * The hostname
   */
  private static String hostname;

  private static List<Metric> metrics = new LinkedList<>();

  /**
   * The service entry point.
   */
  public static void main(String[] arguments) {
    Configurator.initialize("default", CONSOLE_LOG_CONFIG);
    final ConfigStoreOptions fileStore = new ConfigStoreOptions().setType("file").setConfig(new JsonObject().put("path", "config.json"));
    final ConfigStoreOptions envConfig = new ConfigStoreOptions().setType("env");
    final ConfigStoreOptions sysConfig = new ConfigStoreOptions().setType("sys");
    final ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore).addStore(envConfig).addStore(sysConfig);
    boolean debug = Arrays.asList(arguments).contains("--debug");

    final VertxOptions vertxOptions = new VertxOptions()
      .setWorkerPoolSize(NumberUtils.toInt(System.getenv(VERTX_WORKER_POOL_SIZE), 128))
      .setPreferNativeTransport(true);

    if (debug) {
      vertxOptions
          .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(1))
          .setMaxEventLoopExecuteTime(TimeUnit.MINUTES.toMillis(1))
          .setMaxWorkerExecuteTime(TimeUnit.MINUTES.toMillis(1))
          .setWarningExceptionTime(TimeUnit.MINUTES.toMillis(1));
    }

    vertx = Vertx.vertx(vertxOptions);
    webClient = WebClient.create(Service.vertx, new WebClientOptions().setUserAgent(XYZ_HUB_USER_AGENT));
    ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
    retriever.getConfig(Service::onConfigLoaded);
  }

  /**
   *
   */
  private static void onConfigLoaded(AsyncResult<JsonObject> ar) {
    final JsonObject config = ar.result();
    //Convert empty string values to null
    config.forEach(e -> {
      if (e.getValue() != null && e.getValue().equals("")) {
        config.put(e.getKey(), (String) null);
      }
    });
    configuration = config.mapTo(Config.class);

    initializeLogger(configuration);
    decryptSecrets();

    cacheClient = CacheClient.create();

    spaceConfigClient = SpaceConfigClient.getInstance();
    connectorConfigClient = ConnectorConfigClient.getInstance();

    spaceConfigClient.init(spaceConfigReady -> {
      if (spaceConfigReady.succeeded()) {
        connectorConfigClient.init(connectorConfigReady -> {
          if (connectorConfigReady.succeeded()) {
            if (Service.configuration.INSERT_LOCAL_CONNECTORS) {
              connectorConfigClient.insertLocalConnectors(result -> onLocalConnectorsInserted(result, config));
            }
            else {
              onLocalConnectorsInserted(Future.succeededFuture(), config);
            }
          }
        });
      }
    });
  }

  private static void onLocalConnectorsInserted(AsyncResult<Void> result, JsonObject config)  {
    if (result.failed()) {
      logger.error("Failed to insert local connectors.", result.cause());
    }
    else {
      BurstAndUpdateThread.initialize(initializeAr -> onServiceInitialized(initializeAr, config));
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

    final Router global = Router.router(vertx);
    final List<String> verticlesClassNames = Arrays.asList(Service.configuration.VERTICLES_CLASS_NAMES.split(","));
    final Vector<Router> routers = new Vector<>();
    final Future<AsyncResult<Void>> future = Future.future();

    final DeploymentOptions options = new DeploymentOptions()
          .setConfig(config)
          .setWorker(false)
          .setInstances(Runtime.getRuntime().availableProcessors()*2);

    future.compose(r -> {
      final List<Future> futures = new ArrayList<>();

      verticlesClassNames.forEach(className -> {
        final Future<AsyncResult<String>> deployVerticleFuture = Future.future();
        futures.add(deployVerticleFuture);

        logger.info("Deploying verticle: " + className);
        vertx.deployVerticle(className, options, deployVerticleHandler -> {
          if (deployVerticleHandler.failed())
            logger.warn("Unable to load verticle class:" + className);
          deployVerticleFuture.complete();
        });
      });

      return CompositeFuture.all(futures);
    }).setHandler(done -> {
      routers.forEach(router -> {
        global.mountSubRouter("/", router);
      });

      final List<Future> futures = new ArrayList<Future>() {{
        add(createDefaultHttpServer(global));
        add(createAdminHttpServer(global));
      }};

      CompositeFuture.all(futures).setHandler(serversResult -> {
        if (serversResult.succeeded()) {
          logger.info("XYZ Hub " + BUILD_VERSION + " was started at " + new Date().toString());
          logger.info("Native transport enabled: " + vertx.isNativeTransportEnabled());
        }
      });
    });

    // shared data initialization
    vertx.sharedData().getAsyncMap(SHARED_DATA, asyncMapResult -> asyncMapResult.result().put(ROUTERS, routers, future::complete));

    Thread.setDefaultUncaughtExceptionHandler((thread, t) -> logger.error("Uncaught exception: ", t));

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        stopMetricPublishers();
      //This may fail, if we are OOM, but lets at least try.
      logger.warn("XYZ Service is going down at " + new Date().toString());
    }));

      startMetricPublishers();
  }

  private static Future<Void> createDefaultHttpServer(Router router) {
    final Future<Void> future = Future.future();

    vertx.createHttpServer(SERVER_OPTIONS)
        .requestHandler(router)
        .listen(
            Service.configuration.HTTP_PORT, result -> {
              if (result.succeeded()) {
                future.complete();
              } else {
                logger.error("An error occurred, during the initialization of the server.", result.cause());
                future.fail(result.cause());
              }
            });

    return future;
  }

  protected static Future<Void> createAdminHttpServer(Router router) {
    final int messagePort = Service.configuration.ADMIN_MESSAGE_PORT;
    if (messagePort == Service.configuration.HTTP_PORT) {
      return Future.succeededFuture();
    }

    final Future<Void> future = Future.future();

    //Create 2nd HTTP server for admin-messaging
    vertx.createHttpServer(SERVER_OPTIONS)
      .requestHandler(router)
      .listen(messagePort, result -> {
        if (result.succeeded()) {
          logger.debug("HTTP server also listens on admin-messaging port {}.", messagePort);
        }
        else {
          logger.error("An error occurred, during the initialization of admin-messaging http port" + messagePort
                  + ". Messaging won't work correctly.",
              result.cause());
        }
        //Complete in any case as the admin-messaging is not essential
        future.complete();
      });

    return future;
  }

  private static void decryptSecrets() {
    try {
      configuration.STORAGE_DB_PASSWORD = decryptSecret(configuration.STORAGE_DB_PASSWORD);
    } catch (CryptoException e) {
      throw new RuntimeException("Error when trying to decrypt STORAGE_DB_PASSWORD.", e);
    }
    try {
      configuration.ADMIN_MESSAGE_JWT = decryptSecret(configuration.ADMIN_MESSAGE_JWT);
    } catch (CryptoException e) {
      configuration.ADMIN_MESSAGE_JWT = null;
      logger.error("Error when trying to decrypt ADMIN_MESSAGE_JWT. AdminMessaging won't work.", e);
    }
  }

  private static String decryptSecret(String encryptedSecret) throws CryptoException {
    if (ConfigDecryptor.isEncrypted(encryptedSecret)) {
      return ConfigDecryptor.decryptSecret(encryptedSecret);
    }
    return encryptedSecret;
  }

  private static void initializeLogger(Config config) {
    if (!CONSOLE_LOG_CONFIG.equals(config.LOG_CONFIG)) {
      Configurator.reconfigure(NetUtils.toURI(config.LOG_CONFIG));
    }
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

  private static String getBuildProperty(String name) {
    InputStream input = Service.class.getResourceAsStream("/build.properties");

    // load a properties file
    Properties buildProperties = new Properties();
    try {
      buildProperties.load(input);
    } catch (IOException ignored) {
    }

    return buildProperties.getProperty(name);
  }

  private static long getBuildTime() {
    String buildTime = getBuildProperty("xyzhub.buildTime");
    try {
      return new SimpleDateFormat("yyyy.MM.dd-HH:mm").parse(buildTime).getTime();
    } catch (ParseException e) {
      return 0;
    }
  }

  public static long getUsedMemoryBytes() {
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  public static float getUsedMemoryPercent() {
    float used = getUsedMemoryBytes();
    float total = Runtime.getRuntime().totalMemory();
    return used / total * 100;
  }

  public static long currentTimeMillis() {
    return clock.currentTimeMillis();
  }

  private static void startMetricPublishers() {
    if (configuration.PUBLISH_METRICS) {
      metrics.add(new MemoryMetric(new CloudWatchMetricPublisher("XYZ/Hub", "JvmMemoryUtilization",
          "ServiceName", "XYZ-Hub-" + configuration.ENVIRONMENT_NAME, StandardUnit.Percent)));
      metrics.add(new MajorGcCountMetric(new CloudWatchMetricPublisher("XYZ/Hub", "MajorGcCount",
          "ServiceName", "XYZ-Hub-" + configuration.ENVIRONMENT_NAME, StandardUnit.Count)));
    }
  }

  private static void stopMetricPublishers() {
    metrics.forEach(Metric::stop);
  }

  /**
   * The service configuration.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Config {

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
     * The S3 Bucket, which connectors with transfer limitations, could use to relocate responses.
     */
    public String XYZ_HUB_S3_BUCKET;

    /**
     * The public endpoint.
     */
    public String XYZ_HUB_PUBLIC_ENDPOINT;

    /**
     * The redis host.
     */
    public String XYZ_HUB_REDIS_HOST;

    /**
     * The redis port.
     */
    public int XYZ_HUB_REDIS_PORT;

    /**
     * The urls of remote hub services, separated by semicolon ';'
     */
    public String XYZ_HUB_REMOTE_SERVICE_URLS = "";

    /**
     * The redis auth token.
     */
    public String XYZ_HUB_REDIS_AUTH_TOKEN;

    /**
     * The authorization type.
     */
    public Authorization.AuthorizationType XYZ_HUB_AUTH;

    /**
     * The public key used for verifying the signature of the JWT tokens.
     */
    public String JWT_PUB_KEY;

    /**
     * The LOG4J config file location.
     */
    public String LOG_CONFIG;

    /**
     * If set to true, the connectors configuration will be populated with connectors defined in connectors.json.
     */
    public boolean INSERT_LOCAL_CONNECTORS;

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
     * The default timeout for remote functions requests.
     */
    public int REMOTE_FUNCTION_REQUEST_TIMEOUT; //seconds

    /**
     * The amount of executor threads of the pool being shared by all LambdaFunctionClients.
     */
    public int LAMBDA_REMOTE_FUNCTION_EXECUTORS;

    /**
     * The web root for serving static resources from the file system.
     */
    public String FS_WEB_ROOT;

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
     * The verticles class names to be deployed, separated by comma
     */
    public String VERTICLES_CLASS_NAMES;
  }

  /**
   * That message can be used to change the log-level of one or more service-nodes.
   * The specified level must be a valid log-level.
   * As this is a {@link RelayedMessage} it can be sent to a specific service-node or to all service-nodes regardless of the first service
   * node by which it was received.
   *
   * Specifying the property {@link RelayedMessage#relay} to true will relay the message to the specified destination.
   * If no destination is specified the message will be relayed to all service-nodes (broadcast).
   */
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
