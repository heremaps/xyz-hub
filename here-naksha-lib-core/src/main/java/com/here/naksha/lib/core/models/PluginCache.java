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
package com.here.naksha.lib.core.models;

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.exceptions.UncheckedException;
import com.here.naksha.lib.core.lambdas.F1;
import java.lang.reflect.Constructor;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * Cache for plug-ins.
 */
@SuppressWarnings("StringOperationCanBeSimplified")
public final class PluginCache {

  private PluginCache() {
    final Thread thread = new Thread(this::cleanup, "ConstructorCache");
    thread.setDaemon(true);
    thread.start();
  }

  private static final @NotNull PluginCache instance = new PluginCache();

  private static final String CLASS_NOT_FOUND = new String("CLASS_NOT_FOUND");
  private static final String NO_SUCH_METHOD = new String("NO_SUCH_METHOD");
  private static final String API_NOT_SUPPORTED = new String("API_NOT_SUPPORTED");
  private final ConcurrentHashMap<@NotNull String, @NotNull Object> cache = new ConcurrentHashMap<>();

  /**
   * Returns the constructor for the given plugin class, implementing the given API.
   *
   * @param className  the full qualified name of the class.
   * @param apiClass   the API that need to be implemented.
   * @param paramClass the class of the parameter.
   * @param <API>      the API-type.
   * @param <PARAM>    the parameter-type.
   * @return the constructor.
   * @throws ClassNotFoundException if no such class exists.
   * @throws NoSuchMethodException  if the class does not have the required constructor.
   * @throws ClassCastException     if the class does not implement the API.
   */
  @SuppressWarnings({"unchecked", "JavadocDeclaration"})
  public static <API, PARAM> @NotNull F1<API, PARAM> get(
      final @NotNull String className,
      final @NotNull Class<API> apiClass,
      final @NotNull Class<PARAM> paramClass) {
    Object raw = instance.cache.get(className);
    if (raw instanceof F1<?, ?> constructor) {
      return (F1<API, PARAM>) constructor;
    }
    if (raw == null) {
      try {
        final Class<?> theClass = Class.forName(className);
        if (!apiClass.isAssignableFrom(theClass)) {
          raw = API_NOT_SUPPORTED;
        } else {
          try {
            final Constructor<?> constructor = theClass.getConstructor(paramClass);
            final F1<API, PARAM> function = (param) -> {
              try {
                return (API) constructor.newInstance(param);
              } catch (Throwable t) {
                throw unchecked(t);
              }
            };
            instance.cache.putIfAbsent(className, function);
            return function;
          } catch (NoSuchMethodException e) {
            final Constructor<?> constructor = theClass.getConstructor();
            final F1<API, PARAM> function = (param) -> {
              try {
                return (API) constructor.newInstance();
              } catch (Throwable t) {
                throw unchecked(t);
              }
            };
            instance.cache.putIfAbsent(className, function);
            return function;
          }
        }
      } catch (ClassNotFoundException e) {
        raw = CLASS_NOT_FOUND;
      } catch (NoSuchMethodException e) {
        raw = NO_SUCH_METHOD;
      }
      assert raw != null;
      instance.cache.putIfAbsent(className, raw);
    }
    if (raw == NO_SUCH_METHOD) {
      throw unchecked(new NoSuchMethodException(className + "(" + paramClass.getName() + ")"));
    }
    if (raw == API_NOT_SUPPORTED) {
      throw new ClassCastException(className + " does not implement " + apiClass.getName());
    }
    throw unchecked(new ClassNotFoundException(className));
  }

  /**
   * Create a new instance of the given plugin class, implementing the given API.
   *
   * @param className the full qualified name of the class implementing the API.
   * @param apiClass  the API that need to be implemented.
   * @param parameter the class of the parameter.
   * @param <API>     the API-type.
   * @param <PARAM>   the parameter-type.
   * @return the instance.
   * @throws ClassNotFoundException If no such class exists.
   * @throws ClassCastException     If the class does not implement the requested API.
   * @throws NoSuchMethodException  If the class does not have the required constructor.
   * @throws UncheckedException     If the constructor has thrown an exception, use {@link UncheckedException#cause(Throwable)} to unpack.
   */
  @SuppressWarnings("JavadocDeclaration")
  public static <API, PARAM> API newInstance(
      @NotNull String className, @NotNull Class<API> apiClass, @NotNull PARAM parameter) {
    //noinspection unchecked
    final F1<API, PARAM> constructor = get(className, apiClass, (Class<PARAM>) parameter.getClass());
    return constructor.call(parameter);
  }

  private void cleanup() {
    while (true) {
      try {
        final Enumeration<@NotNull String> keys = cache.keys();
        while (keys.hasMoreElements()) {
          final String key = keys.nextElement();
          final Object o = cache.get(key);
          if (!(o instanceof Constructor<?>)) {
            cache.remove(key, o);
          }
        }

        //noinspection BusyWait
        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
      } catch (InterruptedException ignore) {
      } catch (Throwable t) {
        currentLogger()
            .atError("Unexpected exception in plugin cache cleaner")
            .setCause(t)
            .log();
      }
    }
  }
}