package com.here.xyz.hub;

import static com.here.xyz.util.IoHelp.openResource;
import static com.here.xyz.util.IoHelp.parseValue;

import com.here.xyz.hub.auth.NakshaAuthProvider;
import com.here.xyz.hub.util.metrics.GcDurationMetric;
import com.here.xyz.hub.util.metrics.GlobalInflightRequestMemory;
import com.here.xyz.hub.util.metrics.GlobalUsedRfcConnections;
import com.here.xyz.hub.util.metrics.MajorGcCountMetric;
import com.here.xyz.hub.util.metrics.MemoryMetric;
import com.here.xyz.hub.util.metrics.base.CWBareValueMetricPublisher;
import com.here.xyz.hub.util.metrics.base.MetricPublisher;
import com.here.xyz.hub.util.metrics.net.ConnectionMetrics;
import com.here.xyz.hub.util.metrics.net.NakshaHubMetricsFactory;
import com.here.xyz.util.IoHelp;
import com.here.xyz.util.IoHelp.LoadedConfig;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.State;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class NakshaHub {

  /**
   * The logger.
   */
  public static final Logger logger = LoggerFactory.getLogger(NakshaHub.class);

  /**
   * The build properties read from {@code build.properties} file.
   */
  public final @NotNull Properties buildProperties;

  /**
   * The build time.
   */
  public final long buildTime;

  /**
   * The build version.
   */
  public final @NotNull String buildVersion;

  /**
   * The user-agent to send when contacting external services via HTTP.
   */
  public final @NotNull String userAgent;

  /**
   * The unique host identifier.
   */
  public final @NotNull String hostId;

  /**
   * The service configuration.
   */
  public final @NotNull NakshaHubConfig config;

  /**
   * A web client to access XYZ Hub nodes and other web resources.
   */
  public final @NotNull WebClient webClient;

  /**
   * The public endpoint.
   */
  public final @NotNull URL endpoint;

  /**
   * The VertX-Options.
   */
  public final @NotNull VertxOptions vertxOptions;

  /**
   * The VertX-Metrics-Options
   */
  public final @NotNull MetricsOptions vertxMetricsOptions;

  /**
   * The entry point to the Vert.x core API.
   */
  public final @NotNull Vertx vertx;

  /**
   * The authentication options used to generate the {@link #authProvider}.
   */
  public final @NotNull JWTAuthOptions authOptions;
  /**
   * The auth-provider.
   */
  public final @NotNull NakshaAuthProvider authProvider;

  private static @NotNull String jwtPubKey(@NotNull String jwtPubKey) {
    if (!jwtPubKey.startsWith("-----")) {
      jwtPubKey = "-----BEGIN PUBLIC KEY-----\n" + jwtPubKey;
    }
    if (!jwtPubKey.endsWith("-----")) {
      jwtPubKey = jwtPubKey + "\n-----END PUBLIC KEY-----";
    }
    return jwtPubKey;
  }

  /**
   * Create a new Naksha-Hub service instance using the given configuration, but not yet run it. Therefore, the hub is initialized, but not
   * yet bound to a socket nor able to processed requests. To start the service call the method {@link #start()}.
   *
   * @param config The configuration to use.
   * @throws IOException If loading the build properties failed.
   */
  public NakshaHub(@NotNull NakshaHubConfig config) throws IOException {
    this.config = config;
    buildProperties = NakshaHub.getBuildProperties();
    buildVersion = parseValue(buildProperties.get("naksha.version"), String.class);
    final String buildTime = parseValue(buildProperties.get("naksha.buildTime"), String.class);
    try {
      this.buildTime = new SimpleDateFormat("yyyy.MM.dd-HH:mm").parse(buildTime).getTime();
    } catch (ParseException e) {
      throw new IOException("Failed to parse the build time from build.properties resources");
    }
    userAgent = "Naksha/" + buildVersion;
    hostId = UUID.randomUUID().toString();
    vertxMetricsOptions = new MetricsOptions().setEnabled(true).setFactory(new NakshaHubMetricsFactory());
    vertxOptions = new VertxOptions();
    logger.info("Config file location: {} (path={})", config.filename(), config.loadPath());

    if (config.hostname == null || config.hostname.length() == 0) {
      try {
        config.hostname = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        logger.error("Unable to resolve the hostname using Java's API.", e);
        config.hostname = "localhost";
      }
    }

    URL endpoint = null;
    if (config.endpoint != null && config.endpoint.length() > 0) {
      try {
        endpoint = new URL(config.endpoint);
      } catch (MalformedURLException e) {
        logger.error("Invalid configuration of endpoint", e);
      }
    }
    if (endpoint == null) {
      endpoint = new URL("http://" + config.hostname + ":" + config.httpPort);
    }
    this.endpoint = endpoint;

    // See: https://vertx.io/docs/vertx-core/java
    vertxOptions.setMetricsOptions(vertxMetricsOptions);
    vertxOptions.setPreferNativeTransport(true);
    if (config.debug) {
      // If running in debug mode, we need to increase the warning time, because we might enter a break-point for some time!
      vertxOptions
          .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(3))
          .setMaxEventLoopExecuteTime(TimeUnit.MINUTES.toMillis(3))
          .setMaxWorkerExecuteTime(TimeUnit.MINUTES.toMillis(3))
          .setWarningExceptionTime(TimeUnit.MINUTES.toMillis(3));
    }
    vertx = Vertx.vertx(vertxOptions);

    if (config.jwtPubKey != null) {
      final String jwtPubKey = jwtPubKey(config.jwtPubKey);
      authOptions = new JWTAuthOptions().addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwtPubKey));
    } else {
      final LoadedConfig loadedConfig = IoHelp.readConfigFromHomeOrResource("auth/" + config.jwtKeyName() + ".key", config.appName());
      logger.info("Loaded JWT key file {}", loadedConfig.path());
      final String jwt = new String(loadedConfig.bytes(), StandardCharsets.UTF_8);
      authOptions = new JWTAuthOptions()
          .setJWTOptions(new JWTOptions().setAlgorithm("RS256"))
          .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(jwt));
    }
    authProvider = new NakshaAuthProvider(vertx, authOptions);

    final WebClientOptions webClientOptions = new WebClientOptions();
    webClientOptions.setUserAgent(userAgent);
    webClientOptions.setTcpKeepAlive(true).setTcpQuickAck(true).setTcpFastOpen(true);
    webClientOptions.setIdleTimeoutUnit(TimeUnit.MINUTES).setIdleTimeout(2);
    webClient = WebClient.create(vertx, webClientOptions);
    shutdownThread = new Thread(this::shutdownHook);
  }

  private final AtomicBoolean start = new AtomicBoolean();

  /**
   * Tests whether the service is currently running.
   *
   * @return {@code true} if the service is currently running; {@code false} otherwise.
   */
  public boolean isRunning() {
    return start.get() && shutdownThread.getState() == State.NEW;
  }

  /**
   * Start the server.
   *
   * @throws IllegalStateException If the service already started.
   */
  public void start() {
    if (!start.compareAndSet(false, true)) {
      throw new IllegalStateException("Service already started");
    }
    final int processors = Runtime.getRuntime().availableProcessors();
    verticles = new NakshaHubVerticle[processors];
    for (int i = 0; i < processors; i++) {
      verticles[i] = new NakshaHubVerticle(this, i);
      vertx.deployVerticle(verticles[i]);
    }
    Thread.setDefaultUncaughtExceptionHandler(NakshaHub::uncaughtExceptionHandler);
    Runtime.getRuntime().addShutdownHook(this.shutdownThread);
    startMetricPublishers();
  }

  protected @NotNull NakshaHubVerticle[] verticles;

  /**
   * Emergency uncaught exception handler to prevent that the server crashs.
   *
   * @param thread    The thread that cause the exception.
   * @param throwable The exception thrown.
   */
  protected static void uncaughtExceptionHandler(@NotNull Thread thread, @NotNull Throwable throwable) {
    logger.error("Uncaught exception in thread {}", thread.getName(), throwable);
  }

  /**
   * The thread that is registered as shutdown hook.
   */
  protected final @NotNull Thread shutdownThread;

  /**
   * Registered by the constructor
   */
  protected void shutdownHook() {
    final String msg = "Service is going down at " + new Date();
    logger.info(msg);
    System.out.println(msg);
    System.out.flush();
    stopMetricPublishers();
  }

  public static long getUsedMemoryBytes() {
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

  public static float getUsedMemoryPercent() {
    float used = getUsedMemoryBytes();
    float total = Runtime.getRuntime().totalMemory();
    return used / total * 100;
  }

  private final List<MetricPublisher<?>> metricPublishers = new LinkedList<>();

  protected void startMetricPublishers() {
    ConnectionMetrics.initialize();
    metricPublishers.add(new CWBareValueMetricPublisher(new MemoryMetric("JvmMemoryUtilization")));
    metricPublishers.add(new CWBareValueMetricPublisher(new MajorGcCountMetric("MajorGcCount")));
    metricPublishers.add(new CWBareValueMetricPublisher(new GcDurationMetric("GcDuration")));
    metricPublishers.add(new CWBareValueMetricPublisher(new GlobalUsedRfcConnections("GlobalUsedRfcConnections")));
    metricPublishers.add(new CWBareValueMetricPublisher(new GlobalInflightRequestMemory("GlobalInflightRequestMemory")));
    metricPublishers.addAll(ConnectionMetrics.startConnectionMetricPublishers());
  }

  protected void stopMetricPublishers() {
    metricPublishers.forEach(MetricPublisher::stop);
  }


  private static final AtomicReference<@Nullable Boolean> usesZGC = new AtomicReference<>();

  public static boolean isUsingZgc() {
    final Boolean usesZgc = usesZGC.get();
    if (usesZgc != null) {
      return usesZgc;
    }
    List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
      if (gcMxBean.getName().startsWith("ZGC")) {
        usesZGC.set(Boolean.TRUE);
        return true;
      }
    }
    usesZGC.set(Boolean.FALSE);
    return false;
  }

  /**
   * The LOG4J configuration file.
   */
  protected static final String CONSOLE_LOG_CONFIG = "log4j2-console-plain.json";

  /**
   * The Vertx worker pool size environment variable.
   */
  protected static final String VERTX_WORKER_POOL_SIZE = "VERTX_WORKER_POOL_SIZE";

  public static final ThreadFactory newThreadFactory(String groupName) {
    return new DefaultThreadFactory(groupName);
  }

  private static class DefaultThreadFactory implements ThreadFactory {

    private ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    public DefaultThreadFactory(String groupName) {
      assert groupName != null;
      group = new ThreadGroup(groupName);
      namePrefix = groupName + "-";
    }

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(group, r, namePrefix + threadNumber.getAndIncrement());
    }
  }

  /**
   * Read the build properties from the JAR.
   *
   * @return The build properties read from the "build.properties" file.
   */
  public static @NotNull Properties getBuildProperties() throws IOException {
    final InputStream input = openResource("/build.properties");
    // load a properties file
    final Properties buildProperties = new Properties();
    buildProperties.load(input);
    return buildProperties;
  }

  /**
   * A method to let the service die with the given exit-code, error message and optional exception.
   *
   * @param exitCode The exit code to return to the OS.
   * @param reason   The human-readable reason.
   */
  public static void die(final int exitCode, final @NotNull String reason) {
    die(exitCode, reason, null);
  }

  /**
   * A method to let the service die with the given exit-code, error message and optional exception.
   *
   * @param exitCode  The exit code to return to the OS.
   * @param reason    The human-readable reason.
   * @param exception The exception; if any; that caused the exit.
   */
  public static void die(final int exitCode, final @NotNull String reason, @Nullable Throwable exception) {
    // Let's always generate a stack-trace.
    if (exception == null) {
      exception = new RuntimeException();
    }
    logger.error(reason, exception);
    System.out.flush();
    System.err.println(reason);
    exception.printStackTrace(System.err);
    System.err.flush();
    System.exit(exitCode);
  }

}
