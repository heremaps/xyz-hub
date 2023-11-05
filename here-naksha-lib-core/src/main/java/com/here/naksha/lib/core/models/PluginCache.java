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
import com.here.naksha.lib.core.lambdas.*;
import java.lang.reflect.Constructor;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

  private static @Nullable Constructor<?> getConstructorIfAssignable(
      final @NotNull Constructor<?> constructor, final int noOfParams, final @Nullable Class<?>... paramClasses) {
    // actual number of constructor params
    final Class<?>[] cParamTypes = constructor.getParameterTypes();
    int noOfCParams = cParamTypes.length;
    if (noOfParams != noOfCParams) return null;
    // check if all parameters are assignable
    boolean allParamMatches = true;
    for (int i = 0; i < noOfParams; i++) {
      if (paramClasses[i] == null || !cParamTypes[i].isAssignableFrom(paramClasses[i])) {
        allParamMatches = false;
        break;
      }
    }
    if (allParamMatches) return constructor; // we found matching constructor
    return null;
  }

  private static Constructor<?> addConstructorToCache(
      @NotNull String className, int noOfParams, @NotNull Constructor<?> constructor) {
    instance.cache.putIfAbsent(className + ":" + noOfParams, constructor);
    return constructor;
  }

  private static <API> @NotNull Constructor<?> getMatchingConstructor(
      final @NotNull String className,
      final @NotNull Class<API> apiClass,
      final int noOfParams,
      final @Nullable Class<?>... paramClasses) {
    // TODO : can be easily enhanced for additional arguments (when needed)
    if (noOfParams > 4)
      throw new UnsupportedOperationException("Unsupported value " + noOfParams + " for constructor arguments.");
    // check if cache already has assignable constructor
    Object raw = instance.cache.get(className + ":" + noOfParams);
    if (raw instanceof Constructor<?> constructor) {
      if (!apiClass.isAssignableFrom(constructor.getDeclaringClass())) {
        raw = API_NOT_SUPPORTED;
      } else {
        constructor = getConstructorIfAssignable(constructor, noOfParams, paramClasses);
        if (constructor != null) {
          return addConstructorToCache(className, noOfParams, constructor);
        }
      }
    }
    if (raw == null) {
      // not found in cache. need to search for valid constructor
      try {
        final Class<?> theClass = Class.forName(className);
        if (!apiClass.isAssignableFrom(theClass)) {
          raw = API_NOT_SUPPORTED;
        } else {
          // first attempt finding direct parameter matching constructor (ignoring super interface/super class
          // types)
          try {
            final Constructor<?> c =
                switch (noOfParams) {
                  case 4 -> theClass.getConstructor(
                      paramClasses[0], paramClasses[1], paramClasses[2], paramClasses[3]);
                  case 3 -> theClass.getConstructor(
                      paramClasses[0], paramClasses[1], paramClasses[2]);
                  case 2 -> theClass.getConstructor(paramClasses[0], paramClasses[1]);
                  case 1 -> theClass.getConstructor(paramClasses[0]);
                  case 0 -> theClass.getConstructor();
                  default -> throw new UnsupportedOperationException(
                      "Unsupported value " + noOfParams + " for constructor arguments.");
                };
            // if we reach here, then we got direct match
            return addConstructorToCache(className, noOfParams, c);
          } catch (NoSuchMethodException ignored) {
          }

          // now iterate through all other constructors for best suitable match for the given number of params
          for (final Constructor<?> constructor : theClass.getConstructors()) {
            if (noOfParams == constructor.getParameterCount()) {
              final Constructor<?> c = getConstructorIfAssignable(constructor, noOfParams, paramClasses);
              if (c != null) return addConstructorToCache(className, noOfParams, c);
            }
          }

          // we reduce number of params to find next suitable constructor (in recursive manner)
          if (noOfParams > 0) {
            return getMatchingConstructor(className, apiClass, noOfParams - 1, paramClasses);
          }

          raw = NO_SUCH_METHOD;
        }
      } catch (ClassNotFoundException e) {
        raw = CLASS_NOT_FOUND;
      }
      assert raw != null;
      instance.cache.putIfAbsent(className, raw);
    }
    if (raw == NO_SUCH_METHOD) {
      throw unchecked(new NoSuchMethodException(className + "(" + className + ")"));
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
   * @param parameter the first constructor parameter.
   * @param <API>     the API-type.
   * @return the instance.
   * @throws ClassNotFoundException If no such class exists.
   * @throws ClassCastException     If the class does not implement the requested API.
   * @throws NoSuchMethodException  If the class does not have the required constructor.
   * @throws UncheckedException     If the constructor has thrown an exception, use {@link UncheckedException#cause(Throwable)} to unpack.
   */
  @SuppressWarnings("JavadocDeclaration")
  public static <API> API newInstance(
      @NotNull String className, @NotNull Class<API> apiClass, @NotNull Object parameter) {
    final Constructor<?> c = getMatchingConstructor(className, apiClass, 1, parameter.getClass());
    return invokeConstructorWithParams(c, apiClass, parameter);
  }

  /**
   * Create a new instance of the given plugin class, implementing the given API.
   *
   * @param className the full qualified name of the class implementing the API.
   * @param apiClass  the API that need to be implemented.
   * @param param     the first constructor parameter
   * @param param2    the second constructor parameter
   * @param <API>     the API-type.
   * @return the instance.
   * @throws ClassNotFoundException If no such class exists.
   * @throws ClassCastException     If the class does not implement the requested API.
   * @throws NoSuchMethodException  If the class does not have the required constructor.
   * @throws UncheckedException     If the constructor has thrown an exception, use {@link UncheckedException#cause(Throwable)} to unpack.
   */
  @SuppressWarnings("JavadocDeclaration")
  public static <API> API newInstance(
      @NotNull String className, @NotNull Class<API> apiClass, @NotNull Object param, @NotNull Object param2) {
    final Constructor<?> c = getMatchingConstructor(className, apiClass, 2, param.getClass(), param2.getClass());
    return invokeConstructorWithParams(c, apiClass, param, param2);
  }

  /**
   * Create a new instance of the given plugin class, implementing the given API.
   *
   * @param className the full qualified name of the class implementing the API.
   * @param apiClass  the API that need to be implemented.
   * @param param     the first constructor parameter
   * @param param2    the second constructor parameter
   * @param param3    the third constructor parameter
   * @param <API>     the API-type.
   * @return the instance.
   * @throws ClassNotFoundException If no such class exists.
   * @throws ClassCastException     If the class does not implement the requested API.
   * @throws NoSuchMethodException  If the class does not have the required constructor.
   * @throws UncheckedException     If the constructor has thrown an exception, use {@link UncheckedException#cause(Throwable)} to unpack.
   */
  @SuppressWarnings("JavadocDeclaration")
  public static <API> API newInstance(
      @NotNull String className,
      @NotNull Class<API> apiClass,
      @NotNull Object param,
      @NotNull Object param2,
      @NotNull Object param3) {
    final Constructor<?> c =
        getMatchingConstructor(className, apiClass, 3, param.getClass(), param2.getClass(), param3.getClass());
    return invokeConstructorWithParams(c, apiClass, param, param2, param3);
  }

  /**
   * Create a new instance of the given plugin class, implementing the given API.
   *
   * @param className the full qualified name of the class implementing the API.
   * @param apiClass  the API that need to be implemented.
   * @param param     the first constructor parameter
   * @param param2    the second constructor parameter
   * @param param3    the third constructor parameter
   * @param param4    the forth constructor parameter
   * @param <API>     the API-type.
   * @return the instance.
   * @throws ClassNotFoundException If no such class exists.
   * @throws ClassCastException     If the class does not implement the requested API.
   * @throws NoSuchMethodException  If the class does not have the required constructor.
   * @throws UncheckedException     If the constructor has thrown an exception, use {@link UncheckedException#cause(Throwable)} to unpack.
   */
  @SuppressWarnings("JavadocDeclaration")
  public static <API> API newInstance(
      @NotNull String className,
      @NotNull Class<API> apiClass,
      @NotNull Object param,
      @NotNull Object param2,
      @NotNull Object param3,
      @NotNull Object param4) {
    final Constructor<?> c = getMatchingConstructor(
        className, apiClass, 4, param.getClass(), param2.getClass(), param3.getClass(), param4.getClass());
    return invokeConstructorWithParams(c, apiClass, param, param2, param3, param4);
  }

  private static <API> API invokeConstructorWithParams(
      @NotNull Constructor<?> c, @NotNull Class<API> apiClass, @NotNull Object... params) {
    final int noOfParams = c.getParameterCount();
    try {
      switch (noOfParams) {
        case 4 -> {
          //noinspection unchecked
          return (API) c.newInstance(params[0], params[1], params[2], params[3]);
        }
        case 3 -> {
          //noinspection unchecked
          return (API) c.newInstance(params[0], params[1], params[2]);
        }
        case 2 -> {
          //noinspection unchecked
          return (API) c.newInstance(params[0], params[1]);
        }
        case 1 -> {
          //noinspection unchecked
          return (API) c.newInstance(params[0]);
        }
        case 0 -> {
          //noinspection unchecked
          return (API) c.newInstance();
        }
        default -> throw new UnsupportedOperationException(
            "Unsupported value " + noOfParams + " for constructor arguments.");
      }
    } catch (Throwable t) {
      throw unchecked(t);
    }
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
