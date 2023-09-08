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

import com.here.xyz.hub.util.ConfigDecryptor;
import com.here.xyz.hub.util.ConfigDecryptor.CryptoException;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.util.CachedClock;
import org.apache.logging.log4j.core.util.NetUtils;

public class Core {

  private static final Logger logger = LogManager.getLogger();

  /**
   * The entry point to the Vert.x core API.
   */
  public static Vertx vertx;

  /**
   * The shared map used across verticles
   */
  public static final String SHARED_DATA = "SHARED_DATA";
  /**
   * The key to access the global router in the shared data
   */
  public static final String GLOBAL_ROUTER = "GLOBAL_ROUTER";
  protected static Router globalRouter;

  /**
   * A cached clock instance.
   */
  private static final CachedClock clock = CachedClock.instance();
  public static long currentTimeMillis() {
    return clock.currentTimeMillis();
  }

  /**
   * The service start time.
   */
  public static final long START_TIME = currentTimeMillis();

  /**
   * The LOG4J configuration file.
   */
  protected static final String CONSOLE_LOG_CONFIG = "log4j2-console-plain.json";

  /**
   * The Vertx worker pool size environment variable.
   */
  protected static final String VERTX_WORKER_POOL_SIZE = "VERTX_WORKER_POOL_SIZE";

  /**
   * The build time.
   */
  public static final long BUILD_TIME = getBuildTime();

  /**
   * The build version.
   */
  public static final String BUILD_VERSION = getBuildProperty("xyzhub.version");

  public static void initialize(VertxOptions vertxOptions, boolean debug, String configFilename, Handler<JsonObject> handler) {
    Configurator.initialize("default", CONSOLE_LOG_CONFIG);
    final ConfigStoreOptions fileStore = new ConfigStoreOptions().setType("file").setConfig(new JsonObject().put("path", configFilename));
    final ConfigStoreOptions envConfig = new ConfigStoreOptions().setType("env");
    final ConfigStoreOptions sysConfig = new ConfigStoreOptions().setType("sys");
    final ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore).addStore(envConfig).addStore(sysConfig).setScanPeriod(24 * 60 * 1000);

    vertxOptions = (vertxOptions != null ? vertxOptions : new VertxOptions())
            .setWorkerPoolSize(NumberUtils.toInt(System.getenv(Core.VERTX_WORKER_POOL_SIZE), 128))
            .setPreferNativeTransport(true);

    if (debug) {
      vertxOptions
              .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(1))
              .setMaxEventLoopExecuteTime(TimeUnit.MINUTES.toMillis(1))
              .setMaxWorkerExecuteTime(TimeUnit.MINUTES.toMillis(1))
              .setWarningExceptionTime(TimeUnit.MINUTES.toMillis(1));
    }

    vertx = Vertx.vertx(vertxOptions);
    ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
    retriever.getConfig(c -> {
      if (c.failed() || c.result() == null) {
        System.err.println("Unable to load the configuration.");
        System.exit(1);
      }
      JsonObject config = c.result();
      config.forEach(entry -> {
        if (entry.getValue() instanceof String) {
          if (entry.getValue().equals("")) {
            config.put(entry.getKey(), null);
          } else {
            try {
              config.put(entry.getKey(), decryptSecret((String) entry.getValue()));
            } catch (CryptoException e) {
              System.err.println("Unable to decrypt value for key " + entry.getKey());
              e.printStackTrace();
              System.exit(1);
            }
          }
        }
      });
      initializeLogger(config, debug);
      handler.handle(config);
    });
  }

  private static void initializeLogger(JsonObject config, boolean debug) {
    if (!CONSOLE_LOG_CONFIG.equals(config.getString("LOG_CONFIG"))) {
      Configurator.reconfigure(NetUtils.toURI(config.getString("LOG_CONFIG")));
    }
    if (debug)
      changeLogLevel("DEBUG");
  }

  static void changeLogLevel(String level) {
    logger.info("LOG LEVEL UPDATE requested. New level will be: " + level);

    Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.getLevel(level));

    logger.info("LOG LEVEL UPDATE performed. New level is now: " + level);
  }

  private static String decryptSecret(String encryptedSecret) throws CryptoException {
    if (ConfigDecryptor.isEncrypted(encryptedSecret)) {
      return ConfigDecryptor.decryptSecret(encryptedSecret);
    }
    return encryptedSecret;
  }

  public static final ThreadFactory newThreadFactory(String groupName) {
    return new DefaultThreadFactory(groupName);
  }

  protected static void onServiceInitialized(AsyncResult<Void> result, JsonObject config, String verticles) {
    if (result.failed()) {
      logger.error("Failed to initialize Connectors. Service can't be started.", result.cause());
      return;
    }

    if (StringUtils.isEmpty(verticles)) {
      logger.error("At least one Verticle class name should be specified on VERTICLES_CLASS_NAMES. Service can't be started");
      return;
    }

    final List<String> verticlesClassNames = Arrays.asList(verticles.split(","));
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
            logger.warn("Unable to load verticle class:" + className, deployVerticleHandler.cause());
          }
          deployVerticlePromise.complete();
        });
      });

      return CompositeFuture.all(futures);
    }).onComplete(done -> {
      // at this point all verticles were initiated and all routers added as subrouter of globalRouter.
      vertx.eventBus().publish(SHARED_DATA, GLOBAL_ROUTER);

      logger.info("XYZ Hub " + BUILD_VERSION + " was started at " + new Date());
      logger.info("Native transport enabled: " + vertx.isNativeTransportEnabled());
    });

    //Shared data initialization
    vertx.sharedData()
        .getAsyncMap(SHARED_DATA, asyncMapResult -> asyncMapResult.result().put(SHARED_DATA, sharedData, sharedDataPromise));

    Thread.setDefaultUncaughtExceptionHandler((thread, t) -> logger.error("Uncaught exception: ", t));

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

  private static long getBuildTime() {
    String buildTime = getBuildProperty("xyzhub.buildTime");
    try {
      return new SimpleDateFormat("yyyy.MM.dd-HH:mm").parse(buildTime).getTime();
    } catch (ParseException e) {
      return 0;
    }
  }

  protected static String getBuildProperty(String name) {
    InputStream input = AbstractHttpServerVerticle.class.getResourceAsStream("/build.properties");

    // load a properties file
    Properties buildProperties = new Properties();
    try {
      buildProperties.load(input);
    } catch (IOException ignored) {
    }

    return buildProperties.getProperty(name);
  }
}