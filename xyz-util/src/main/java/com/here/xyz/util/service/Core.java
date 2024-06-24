/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.util.service;

import com.google.common.primitives.Ints;
import com.here.xyz.util.service.ConfigDecryptor.CryptoException;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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
   * The resource file that contains the build info properties
   */
  protected static String BUILD_PROPERTIES_FILE = "/build.properties";

  /**
   * The build time.
   */
  private static long BUILD_TIME;

  /**
   * The build version.
   */
  private static String BUILD_VERSION;

  public static long buildTime() {
    if (BUILD_TIME == 0)
      BUILD_TIME = loadBuildTime(BUILD_PROPERTIES_FILE);
    return BUILD_TIME;
  }

  protected static Supplier<String> BUILD_VERSION_PROVIDER = () -> {
    if (BUILD_VERSION == null)
      BUILD_VERSION = loadBuildVersion(BUILD_PROPERTIES_FILE);
    return BUILD_VERSION;
  };

  public static String buildVersion() {
    return BUILD_VERSION_PROVIDER.get();
  }

  /**
   * The default config file
   */
  public static String CONFIG_FILE = "config.json";

  public static boolean isDebugModeActive;

  public static Future<Vertx> initializeVertx(VertxOptions vertxOptions) {
    final String workerPoolSize = System.getenv(Core.VERTX_WORKER_POOL_SIZE);
    vertxOptions = (vertxOptions != null ? vertxOptions : new VertxOptions())
            .setWorkerPoolSize(Optional.ofNullable(workerPoolSize == null ? null : Ints.tryParse(workerPoolSize)).orElse(128))
            .setPreferNativeTransport(true);

    if (isDebugModeActive) {
      vertxOptions
              .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(1))
              .setMaxEventLoopExecuteTime(TimeUnit.MINUTES.toMillis(1))
              .setMaxWorkerExecuteTime(TimeUnit.MINUTES.toMillis(1))
              .setWarningExceptionTime(TimeUnit.MINUTES.toMillis(1));
    }

    vertx = Vertx.vertx(vertxOptions);
    return Future.succeededFuture(Vertx.vertx(vertxOptions));
  }

  public static Future<JsonObject> initializeConfig(Vertx vertx) {
    final ConfigStoreOptions fileStore = new ConfigStoreOptions().setType("file").setConfig(new JsonObject().put("path", CONFIG_FILE));
    final ConfigStoreOptions envConfig = new ConfigStoreOptions().setType("env");
    final ConfigStoreOptions sysConfig = new ConfigStoreOptions().setType("sys");
    final ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore).addStore(envConfig).addStore(sysConfig)
        .setScanPeriod(24 * 60 * 1000);

    ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
    return retriever.getConfig()
        .map(config -> {
          config.forEach(entry -> {
            if (entry.getValue() instanceof String) {
              if (entry.getValue().equals("")) {
                config.put(entry.getKey(), null);
              }
              else {
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
          return config;
        })
        .onFailure(e -> {
          System.err.println("Unable to load the configuration.");
          e.printStackTrace();
          System.exit(1);
        });
  }

  public static Future<JsonObject> initializeLogger(JsonObject config) {
    if (!CONSOLE_LOG_CONFIG.equals(config.getString("LOG_CONFIG")))
      Configurator.reconfigure(NetUtils.toURI(config.getString("LOG_CONFIG")));

    if (isDebugModeActive)
      changeLogLevel("DEBUG");

    return Future.succeededFuture(config);
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

  private static long loadBuildTime(String propertiesFile) {
    String buildTime = loadBuildProperty(propertiesFile, "build.time");
    try {
      return new SimpleDateFormat("yyyy.MM.dd-HH:mm").parse(buildTime).getTime();
    }
    catch (ParseException e) {
      return 0;
    }
  }

  protected static String loadBuildVersion(String propertiesFile) {
    return loadBuildProperty(propertiesFile, "build.version");
  }

  private static String loadBuildProperty(String propertiesFile, String name) {
    InputStream input = Core.class.getResourceAsStream(propertiesFile);

    //Load the according properties file
    Properties buildProperties = new Properties();
    try {
      buildProperties.load(input);
    }
    catch (IOException ignored) {}

    return buildProperties.getProperty(name);
  }
}