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
package com.here.naksha.lib.core.models;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.lambdas.Fe3;
import com.here.naksha.lib.core.storage.IStorage;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cache for plug-ins.
 */
@SuppressWarnings({"StringOperationCanBeSimplified", "DuplicatedCode"})
public final class PluginCache {

  static class EventHandlerConstructorByTarget
      extends ConcurrentHashMap<Class<?>, Fe3<IEventHandler, INaksha, ?, ?>> {}

  static class EventHandlerConstructorByConfig extends ConcurrentHashMap<Class<?>, EventHandlerConstructorByTarget> {}

  static class EventHandlerConstructorByClassNameMap
      extends ConcurrentHashMap<String, EventHandlerConstructorByConfig> {}

  static class StorageConstructorByConfig extends ConcurrentHashMap<Class<?>, Fe1<IStorage, ?>> {}

  static class StorageConstructorByClassNameMap extends ConcurrentHashMap<String, StorageConstructorByConfig> {}

  static final EventHandlerConstructorByClassNameMap eventHandlerConstructors =
      new EventHandlerConstructorByClassNameMap();
  static final StorageConstructorByClassNameMap storageConstructors = new StorageConstructorByClassNameMap();

  // **********Extension cache****************
  static class ExtensionConstructorByClassNameMap
      extends ConcurrentHashMap<String, EventHandlerConstructorByTarget> {}

  static ConcurrentHashMap<String, ExtensionConstructorByClassNameMap> extensionCache = new ConcurrentHashMap<>();
  // ******************************************

  /**
   * Wraps the given constructor of an event-handler into a standard function call.
   *
   * @param constructor The constructor.
   * @param configClass The configuration-type class.
   * @param targetClass The target-type class.
   * @param <CONFIG>    The config-type.
   * @param <TARGET>    The target-type.
   * @return the constructor or {@link null}, if this constructor can't be invoked for the given target.
   */
  static <CONFIG, TARGET> @Nullable Fe3<IEventHandler, INaksha, CONFIG, TARGET> wrapEventHandlerConstructor(
      @NotNull Constructor<? extends IEventHandler> constructor,
      @NotNull Class<CONFIG> configClass,
      @NotNull Class<TARGET> targetClass) {
    if (constructor.getParameterCount() > 3) {
      return null;
    }
    final Class<?>[] parameterTypes = constructor.getParameterTypes();
    assert parameterTypes.length <= 3;

    if (parameterTypes.length == 0) {
      return ((naksha, config, target) -> constructor.newInstance());
    }

    if (parameterTypes.length == 1) {
      if (INaksha.class.isAssignableFrom(parameterTypes[0])) {
        return ((naksha, config, target) -> constructor.newInstance(naksha));
      }
      if (configClass.isAssignableFrom(parameterTypes[0])) {
        return ((naksha, config, target) -> constructor.newInstance(config));
      }
      if (targetClass.isAssignableFrom(parameterTypes[0])) {
        return ((naksha, config, target) -> constructor.newInstance(target));
      }
      return null;
    }

    if (parameterTypes.length == 2) {
      if (INaksha.class.isAssignableFrom(parameterTypes[0])) {
        if (configClass.isAssignableFrom(parameterTypes[1])) {
          return ((naksha, config, target) -> constructor.newInstance(naksha, config));
        }
        if (targetClass.isAssignableFrom(parameterTypes[1])) {
          return ((naksha, config, target) -> constructor.newInstance(naksha, target));
        }
        return null;
      }
      if (configClass.isAssignableFrom(parameterTypes[0])) {
        if (INaksha.class.isAssignableFrom(parameterTypes[1])) {
          return ((naksha, config, target) -> constructor.newInstance(config, naksha));
        }
        if (targetClass.isAssignableFrom(parameterTypes[1])) {
          return ((naksha, config, target) -> constructor.newInstance(config, target));
        }
        return null;
      }
      if (targetClass.isAssignableFrom(parameterTypes[0])) {
        if (INaksha.class.isAssignableFrom(parameterTypes[1])) {
          return ((naksha, config, target) -> constructor.newInstance(target, naksha));
        }
        if (configClass.isAssignableFrom(parameterTypes[1])) {
          return ((naksha, config, target) -> constructor.newInstance(target, config));
        }
        return null;
      }
      return null;
    }

    if (INaksha.class.isAssignableFrom(parameterTypes[0])) {
      if (configClass.isAssignableFrom(parameterTypes[1])) {
        if (targetClass.isAssignableFrom(parameterTypes[2])) {
          return (constructor::newInstance); // -> constructor.newInstance(naksha, config, target));
        }
        return null;
      }
      if (targetClass.isAssignableFrom(parameterTypes[1])) {
        if (configClass.isAssignableFrom(parameterTypes[2])) {
          return ((naksha, config, target) -> constructor.newInstance(naksha, target, config));
        }
      }
      return null;
    }
    if (configClass.isAssignableFrom(parameterTypes[0])) {
      if (INaksha.class.isAssignableFrom(parameterTypes[1])) {
        if (parameterTypes[2].isAssignableFrom(targetClass)) { // target: Space, param[2]: EventTarget
          return ((naksha, config, target) -> constructor.newInstance(config, naksha, target));
        }
        return null;
      }
      if (targetClass.isAssignableFrom(parameterTypes[1])) {
        if (INaksha.class.isAssignableFrom(parameterTypes[2])) {
          return ((naksha, config, target) -> constructor.newInstance(config, target, naksha));
        }
      }
      return null;
    }
    if (targetClass.isAssignableFrom(parameterTypes[0])) {
      if (INaksha.class.isAssignableFrom(parameterTypes[1])) {
        if (configClass.isAssignableFrom(parameterTypes[2])) {
          return ((naksha, config, target) -> constructor.newInstance(target, naksha, config));
        }
        return null;
      }
      if (configClass.isAssignableFrom(parameterTypes[1])) {
        if (INaksha.class.isAssignableFrom(parameterTypes[2])) {
          return ((naksha, config, target) -> constructor.newInstance(target, config, naksha));
        }
      }
      return null;
    }
    return null;
  }

  /**
   * Wraps the given constructor of an event-handler into a standard function call.
   *
   * @param constructor The constructor.
   * @param configClass The config-class.
   * @param <CONFIG>    The config-type.
   * @return the constructor or {@link null}, if this constructor can't be invoked for the given target.
   */
  static <CONFIG> @Nullable Fe1<IStorage, CONFIG> wrapStorageConstructor(
      @NotNull Constructor<? extends IStorage> constructor, @NotNull Class<CONFIG> configClass) {
    if (constructor.getParameterCount() > 1) {
      return null;
    }
    final Class<?>[] parameterTypes = constructor.getParameterTypes();
    assert parameterTypes.length <= 1;

    if (parameterTypes.length == 0) {
      return ((config) -> constructor.newInstance());
    }

    if (configClass.isAssignableFrom(parameterTypes[0])) {
      return constructor::newInstance;
    }
    return null;
  }

  static <CONFIG, TARGET> @NotNull
      ConcurrentHashMap<Class<TARGET>, Fe3<IEventHandler, INaksha, CONFIG, TARGET>> eventHandlerConstructorMap(
          @NotNull String className, @NotNull Class<CONFIG> configClass, @NotNull Class<TARGET> targetClass) {
    EventHandlerConstructorByConfig byConfig = eventHandlerConstructors.get(className);
    if (byConfig == null) {
      byConfig = new EventHandlerConstructorByConfig();
      final EventHandlerConstructorByConfig existing = eventHandlerConstructors.putIfAbsent(className, byConfig);
      if (existing != null) {
        byConfig = existing;
      }
    }
    EventHandlerConstructorByTarget byTarget = byConfig.get(configClass);
    if (byTarget == null) {
      byTarget = new EventHandlerConstructorByTarget();
      final EventHandlerConstructorByTarget existing = byConfig.putIfAbsent(configClass, byTarget);
      if (existing != null) {
        byTarget = existing;
      }
    }
    //noinspection unchecked,rawtypes
    return (ConcurrentHashMap<Class<TARGET>, Fe3<IEventHandler, INaksha, CONFIG, TARGET>>)
        (ConcurrentHashMap) byTarget;
  }

  static <CONFIG> @NotNull ConcurrentHashMap<Class<CONFIG>, Fe1<IStorage, CONFIG>> storageConstructorMap(
      @NotNull String className, @NotNull Class<CONFIG> configClass) {
    StorageConstructorByConfig storageConstructorByConfig = storageConstructors.get(className);
    if (storageConstructorByConfig == null) {
      storageConstructorByConfig = new StorageConstructorByConfig();
      StorageConstructorByConfig existing =
          storageConstructors.putIfAbsent(className, storageConstructorByConfig);
      if (existing != null) {
        storageConstructorByConfig = existing;
      }
    }
    //noinspection unchecked,rawtypes
    return (@NotNull ConcurrentHashMap<Class<CONFIG>, Fe1<IStorage, CONFIG>>)
        (ConcurrentHashMap) storageConstructorByConfig;
  }

  /**
   * Returns the constructor for the space event handler.
   *
   * @param className   The classname to search for.
   * @param configClass The configuration-type class, for example {@code EventHandler.class}.
   * @param targetClass The target-type class, for example {@code Space.class}.
   * @param <CONFIG>    The config-type.
   * @param <TARGET>    The target-type.
   * @return the constructor for the event handler.
   * @throws ClassNotFoundException If no such class exists (invalid {@code className}).
   * @throws ClassCastException     If the class does not implement the {@link IEventHandler} interface.
   * @throws NoSuchMethodException  If the class does not have a matching constructor.
   */
  public static <CONFIG, TARGET> @NotNull Fe3<IEventHandler, INaksha, CONFIG, TARGET> getEventHandlerConstructor(
      final @NotNull String className, Class<CONFIG> configClass, Class<TARGET> targetClass) {
    final ConcurrentHashMap<Class<TARGET>, Fe3<IEventHandler, INaksha, CONFIG, TARGET>> constructorByTarget =
        eventHandlerConstructorMap(className, configClass, targetClass);
    Fe3<IEventHandler, INaksha, CONFIG, TARGET> c = constructorByTarget.get(targetClass);
    if (c != null) {
      return c;
    }
    synchronized (PluginCache.class) {
      c = constructorByTarget.get(targetClass);
      if (c != null) {
        return c;
      }
      try {
        final Class<?> theClass = Class.forName(className);
        if (!IEventHandler.class.isAssignableFrom(theClass)) {
          throw new ClassCastException(
              "The class " + theClass.getName() + " does not implement the IEventHandler interface");
        }
        //noinspection unchecked
        final Constructor<? extends IEventHandler>[] constructors =
            (Constructor<IEventHandler>[]) theClass.getConstructors();
        int cParameterCount = -1;
        for (final Constructor<? extends IEventHandler> constructor : constructors) {
          if (constructor.getParameterCount() < cParameterCount) {
            continue;
          }
          if (constructor.getParameterCount() > 3) {
            continue;
          }
          c = wrapEventHandlerConstructor(constructor, configClass, targetClass);
          if (c != null) {
            cParameterCount = constructor.getParameterCount();
          }
        }
        if (c == null) {
          throw new NoSuchMethodException(
              "The class " + theClass.getName() + " does not have a valid constructor");
        }
        constructorByTarget.put(targetClass, c);
        return c;
      } catch (Throwable t) {
        throw unchecked(t);
      }
    }
  }

  /**
   * Returns the constructor for the storage.
   *
   * @param className   The classname to search for.
   * @param configClass The configuration-type class, for example {@code Storage.class}.
   * @param <CONFIG>    The config-type.
   * @return the constructor for the storage.
   * @throws ClassNotFoundException If no such class exists (invalid {@code className}).
   * @throws ClassCastException     If the class does not implement the {@link IEventHandler} API.
   * @throws NoSuchMethodException  If the class does not have the required constructor.
   */
  public static <CONFIG> @NotNull Fe1<IStorage, CONFIG> getStorageConstructor(
      @NotNull String className, @NotNull Class<CONFIG> configClass) {
    final ConcurrentHashMap<Class<CONFIG>, Fe1<IStorage, CONFIG>> map =
        storageConstructorMap(className, configClass);
    Fe1<IStorage, CONFIG> c = map.get(configClass);
    if (c != null) {
      return c;
    }
    synchronized (PluginCache.class) {
      c = map.get(configClass);
      if (c != null) {
        return c;
      }
      try {
        final Class<?> theClass = Class.forName(className);
        if (!IStorage.class.isAssignableFrom(theClass)) {
          throw new ClassCastException(
              "The class " + theClass.getName() + " does not implement the IStorage interface");
        }
        Fe1<IStorage, CONFIG> noParamConstructorCall = null;
        Fe1<IStorage, CONFIG> configParamConstructorCall = null;
        for (final Constructor<? extends IStorage> constructor :
            (Constructor<IStorage>[]) theClass.getConstructors()) {
          if (constructor.getParameterCount() == 0) {
            noParamConstructorCall = constructor::newInstance;
          } else if (constructor.getParameterCount() == 1
              && constructor.getParameterTypes()[0].isAssignableFrom(configClass)) {
            configParamConstructorCall = constructor::newInstance;
          }
        }
        if (configParamConstructorCall != null) {
          map.put(configClass, configParamConstructorCall);
          return configParamConstructorCall;
        } else if (noParamConstructorCall != null) {
          return noParamConstructorCall;
        } else {
          throw new NoSuchMethodException(
              "The class " + theClass.getName() + " does not valid a valid constructor");
        }
      } catch (Throwable t) {
        throw unchecked(t);
      }
    }
  }

  /**
   * Returns the constructor for the Extensions loaded via event handler.
   *
   * @param className   The classname to search for.
   * @param configClass The configuration-type class, for example {@code EventHandler.class}.
   * @param targetClass The target-type class, for example {@code Space.class}.
   * @param extClassLoader Extension class loader
   * @param extensionId Extension identifier
   * @param <CONFIG>    The config-type.
   * @param <TARGET>    The target-type.
   * @return the constructor for the event handler.
   * @throws ClassNotFoundException If no such class exists (invalid {@code className}).
   * @throws ClassCastException     If the class does not implement the {@link IEventHandler} interface.
   * @throws NoSuchMethodException  If the class does not have a matching constructor.
   */
  public static <CONFIG, TARGET> @NotNull Fe3<IEventHandler, INaksha, CONFIG, TARGET> getEventHandlerConstructor(
      final @NotNull String className,
      Class<CONFIG> configClass,
      Class<TARGET> targetClass,
      final @NotNull String extensionId,
      final @NotNull ClassLoader extClassLoader) {

    final ConcurrentHashMap<Class<TARGET>, Fe3<IEventHandler, INaksha, CONFIG, TARGET>> constructorByTarget =
        extensionConstructorMap(extensionId, className);
    Fe3<IEventHandler, INaksha, CONFIG, TARGET> c = constructorByTarget.get(targetClass);
    if (c != null) {
      return c;
    }
    synchronized (PluginCache.class) {
      c = constructorByTarget.get(targetClass);
      if (c != null) {
        return c;
      }
      try {
        final Class<?> theClass = extClassLoader.loadClass(className);
        if (!IEventHandler.class.isAssignableFrom(theClass)) {
          throw new ClassCastException(
              "The class " + theClass.getName() + " does not implement the IEventHandler interface");
        }
        //noinspection unchecked
        final Constructor<? extends IEventHandler>[] constructors =
            (Constructor<IEventHandler>[]) theClass.getConstructors();
        int cParameterCount = -1;
        for (final Constructor<? extends IEventHandler> constructor : constructors) {
          if (constructor.getParameterCount() < cParameterCount) {
            continue;
          }
          if (constructor.getParameterCount() > 3) {
            continue;
          }
          c = wrapEventHandlerConstructor(constructor, configClass, targetClass);
          if (c != null) {
            cParameterCount = constructor.getParameterCount();
          }
        }
        if (c == null) {
          throw new NoSuchMethodException(
              "The class " + theClass.getName() + " does not have a valid constructor");
        }
        constructorByTarget.put(targetClass, c);
        return c;
      } catch (Throwable t) {
        throw unchecked(t);
      }
    }
  }

  static <CONFIG, TARGET> @NotNull
      ConcurrentHashMap<Class<TARGET>, Fe3<IEventHandler, INaksha, CONFIG, TARGET>> extensionConstructorMap(
          final @NotNull String extensionId, final @NotNull String className) {
    ExtensionConstructorByClassNameMap byClassNameMap = extensionCache.get(extensionId);
    if (byClassNameMap == null) {
      byClassNameMap = new ExtensionConstructorByClassNameMap();
      final ExtensionConstructorByClassNameMap existing = extensionCache.putIfAbsent(extensionId, byClassNameMap);
      if (existing != null) {
        byClassNameMap = existing;
      }
    }
    EventHandlerConstructorByTarget byTarget = byClassNameMap.get(className);
    if (byTarget == null) {
      byTarget = new EventHandlerConstructorByTarget();
      final EventHandlerConstructorByTarget existing = byClassNameMap.putIfAbsent(className, byTarget);
      if (existing != null) {
        byTarget = existing;
      }
    }
    //noinspection unchecked,rawtypes
    return (ConcurrentHashMap<Class<TARGET>, Fe3<IEventHandler, INaksha, CONFIG, TARGET>>)
        (ConcurrentHashMap) byTarget;
  }

  /**
   * Method to remove cached extension of given extensionId
   * @param extensionId Extension identifier
   */
  public static void removeExtensionCache(final @NotNull String extensionId) {
    extensionCache.remove(extensionId);
  }
}
