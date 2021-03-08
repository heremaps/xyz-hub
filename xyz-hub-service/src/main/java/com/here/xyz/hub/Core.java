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

import com.here.xyz.hub.util.ConfigDecryptor;
import com.here.xyz.hub.util.ConfigDecryptor.CryptoException;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
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

  public static void initialize(boolean debug, String configFilename, Handler<JsonObject> handler) {
    Configurator.initialize("default", CONSOLE_LOG_CONFIG);
    final ConfigStoreOptions fileStore = new ConfigStoreOptions().setType("file").setConfig(new JsonObject().put("path", configFilename));
    final ConfigStoreOptions envConfig = new ConfigStoreOptions().setType("env");
    final ConfigStoreOptions sysConfig = new ConfigStoreOptions().setType("sys");
    final ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(fileStore).addStore(envConfig).addStore(sysConfig);

    final VertxOptions vertxOptions = new VertxOptions()
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
            config.put(entry.getKey(), (String) null);
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
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig rootConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
    rootConfig.setLevel(Level.getLevel(level));

    //This causes all Loggers to re-fetch information from their LoggerConfig.
    context.updateLoggers();
    logger.info("LOG LEVEL UPDATE performed. New level is now: " + level);
  }

  private static String decryptSecret(String encryptedSecret) throws CryptoException {
    if (ConfigDecryptor.isEncrypted(encryptedSecret)) {
      return ConfigDecryptor.decryptSecret(encryptedSecret);
    }
    return encryptedSecret;
  }
}