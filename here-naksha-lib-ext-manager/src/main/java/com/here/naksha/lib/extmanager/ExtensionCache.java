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
package com.here.naksha.lib.extmanager;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.SimpleTask;
import com.here.naksha.lib.core.models.ExtensionConfig;
import com.here.naksha.lib.core.models.PluginCache;
import com.here.naksha.lib.core.models.features.Extension;
import com.here.naksha.lib.extmanager.helpers.AmazonS3Helper;
import com.here.naksha.lib.extmanager.helpers.ClassLoaderHelper;
import com.here.naksha.lib.extmanager.helpers.FileHelper;
import com.here.naksha.lib.extmanager.models.KVPair;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Class contains registered extensions in naksha. It update/maintain extensions cache over scheduled time.
 */
public class ExtensionCache {
  private static final @NotNull Logger logger = LoggerFactory.getLogger(ExtensionCache.class);
  private static final ConcurrentHashMap<String, KVPair<Extension, ClassLoader>> loaderCache =
      new ConcurrentHashMap<>();
  private static final Map<String, FileClient> jarClientMap = new HashMap<>();
  private final @NotNull INaksha naksha;

  static {
    jarClientMap.put(JarClientType.S3.getType(), new AmazonS3Helper());
    jarClientMap.put(JarClientType.FILE.getType(), new FileHelper());
  }

  public ExtensionCache(@NotNull INaksha naksha) {
    this.naksha = naksha;
  }
  /**
   * Read extensions from database, download respective jars from configured client and store Extension to ClassLoader mapping
   * If it already have any mapping exist for extension then it simply skip that.
   * Also it removes existing mapping from cache which is not available in config store anymore
   */
  protected void buildExtensionCache(ExtensionConfig extensionConfig) {
    List<Future<KVPair<Extension, File>>> futures = extensionConfig.getExtensions().stream()
        .filter(extension -> !this.isLoaderMappingExist(extension))
        .map(extension -> {
          SimpleTask<KVPair<Extension, File>> task = new SimpleTask<>();
          return task.start(() -> downloadJar(extension));
        })
        .toList();

    futures.forEach(future -> {
      KVPair<Extension, File> result = null;
      try {
        result = future.get();
      } catch (InterruptedException | ExecutionException e) {
        logger.error("Failed while downloading extension jar", e);
      }
      publishIntoCache(result, extensionConfig);
    });

    // Removing existing extension which has been removed from the configuration
    List<String> extIds =
        extensionConfig.getExtensions().stream().map(Extension::getId).toList();

    for (String key : loaderCache.keySet()) {
      if (!extIds.contains(key)) {
        loaderCache.remove(key);
        PluginCache.removeExtensionCache(key);
        logger.info("Extension {} removed from cache.", key);
      }
    }
    logger.info("Extension cache size " + loaderCache.size());
  }

  private void publishIntoCache(KVPair<Extension, File> result, ExtensionConfig extensionConfig) {
    if (result != null && result.getValue() != null) {
      final Extension extension = result.getKey();
      final File jarFile = result.getValue();
      ClassLoader loader;
      try {
        loader = ClassLoaderHelper.getClassLoader(jarFile, extensionConfig.getWhilelistDelegateClass());
      } catch (Exception e) {
        logger.error("Failed to load extension jar " + extension.getId(), e);
        return;
      }

      if (!isNullOrEmpty(extension.getInitClassName())) {
        try {
          Class<?> clz = loader.loadClass(extension.getInitClassName());
          clz.getConstructor(INaksha.class, Extension.class).newInstance(naksha, extension);
        } catch (ClassNotFoundException
            | InvocationTargetException
            | InstantiationException
            | NoSuchMethodException
            | IllegalAccessException e) {
          logger.error(
              "Failed to instantiate class {} for extension {} ",
              extension.getInitClassName(),
              extension.getId(),
              e);
          return;
        }
      }
      if (!isNullOrEmpty(extension.getInitClassName()))
        logger.info(
            "Extension {} initialization using initClassName {} done successfully.",
            extension.getId(),
            extension.getInitClassName());
      loaderCache.put(extension.getId(), new KVPair<Extension, ClassLoader>(extension, loader));
      PluginCache.removeExtensionCache(extension.getId());
      logger.info(
          "Extension {},{} is successfully loaded into the cache.",
          extension.getId(),
          extension.getVersion());
    }
  }

  private boolean isLoaderMappingExist(Extension extension) {
    KVPair<Extension, ClassLoader> existingMapping = loaderCache.get(extension.getId());
    if (existingMapping == null) return false;

    final Extension exExtension = existingMapping.getKey();
    final String exInitClassName =
        isNullOrEmpty(exExtension.getInitClassName()) ? "" : exExtension.getInitClassName();
    final String initClassName = isNullOrEmpty(extension.getInitClassName()) ? "" : extension.getInitClassName();

    return exExtension.getUrl().equals(extension.getUrl())
        && exExtension.getVersion().equals(extension.getVersion())
        && exInitClassName.equals(initClassName);
  }

  /**
   * Lamda function which will initiate the downloading for extension jar
   */
  private KVPair<Extension, File> downloadJar(Extension extension) {
    logger.info("Downloading jar {} with version {} ", extension.getId(), extension.getVersion());
    FileClient client = getJarClient(extension.getUrl());
    File file = null;
    try {
      file = client.getFile(extension.getUrl());
    } catch (IOException | SdkClientException e) {
      logger.error("Failed to fetch jar {} ", extension.getUrl());
    }
    return new KVPair<Extension, File>(extension, file);
  }

  // TODO: Can be moved to factory function. Since not used elsewhere placed it inside this class
  protected FileClient getJarClient(String url) {
    if (url.startsWith(JarClientType.S3.getType())) {
      return jarClientMap.get(JarClientType.S3.getType());
    } else if (url.startsWith(JarClientType.FILE.getType())) {
      return jarClientMap.get(JarClientType.FILE.getType());
    } else throw new UnsupportedOperationException("Jar client not configured for url " + url);
  }

  protected ClassLoader getClassLoaderById(@NotNull String extensionId) {
    KVPair<Extension, ClassLoader> mappedLoader = loaderCache.get(extensionId);
    return mappedLoader == null ? null : mappedLoader.getValue();
  }

  public int getCacheLength() {
    return loaderCache.size();
  }

  public List<Extension> getCachedExtensions() {
    return loaderCache.values().stream().map(KVPair::getKey).toList();
  }

  private boolean isNullOrEmpty(String value) {
    return value == null || value.isEmpty();
  }

  public void clear() {
    loaderCache.clear();
  }

  public enum JarClientType {
    S3("s3:"),
    FILE("file:");

    private final String type;

    JarClientType(String type) {
      this.type = type;
    }

    public String getType() {
      return this.type;
    }
  }
}
