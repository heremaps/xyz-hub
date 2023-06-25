package com.here.naksha.lib.core.models;

import static com.here.naksha.lib.core.NakshaContext.currentLogger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/** Cache for plug-ins. */
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
   * @param className the full qualified name of the class.
   * @param apiClass the API that need to be implemented.
   * @param paramClass the class of the parameter.
   * @param <API> the API-type.
   * @param <PARAM> the parameter-type.
   * @return the constructor.
   * @throws ClassNotFoundException if no such class exists, or the class does not implement the
   *     requested API.
   * @throws NoSuchMethodException if the class does not have the required constructor.
   */
  public static <API, PARAM> @NotNull Constructor<API> get(
      @NotNull String className, @NotNull Class<API> apiClass, @NotNull Class<PARAM> paramClass)
      throws ClassNotFoundException, NoSuchMethodException {
    Object raw = instance.cache.get(className);
    if (raw == null) {
      try {
        final Class<?> theClass = Class.forName(className);
        if (!apiClass.isAssignableFrom(theClass)) {
          raw = API_NOT_SUPPORTED;
        } else {
          // TODO: We can use any constructor that uses directly paramClass or any of its super
          // classes!
          raw = theClass.getConstructor(paramClass);
        }
      } catch (ClassNotFoundException e) {
        raw = CLASS_NOT_FOUND;
      } catch (NoSuchMethodException e) {
        raw = NO_SUCH_METHOD;
      }
    }
    instance.cache.putIfAbsent(className, raw);
    if (raw instanceof Constructor<?> constructor) {
      //noinspection unchecked
      return (Constructor<API>) constructor;
    }
    if (raw == NO_SUCH_METHOD) {
      throw new NoSuchMethodException(className + "(" + paramClass.getName() + ")");
    }
    if (raw == API_NOT_SUPPORTED) {
      throw new ClassNotFoundException(className + " does not implement " + apiClass.getName());
    }
    throw new ClassNotFoundException(className);
  }

  /**
   * Create a new instance of the given plugin class, implementing the given API.
   *
   * @param className the full qualified name of the class implementing the API.
   * @param apiClass the API that need to be implemented.
   * @param parameter the class of the parameter.
   * @param <API> the API-type.
   * @param <PARAM> the parameter-type.
   * @return the instance.
   * @throws ClassNotFoundException if no such class exists, or the class does not implement the
   *     requested API.
   * @throws NoSuchMethodException if the class does not have the required constructor.
   * @throws InstantiationException if the creation of the instance fails for internal reasons.
   * @throws InvocationTargetException if the constructor has thrown an exception, use {@link
   *     Exception#getCause()} to query it.
   */
  public static <API, PARAM> API newInstance(
      @NotNull String className, @NotNull Class<API> apiClass, @NotNull PARAM parameter)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException {
    try {
      return get(className, apiClass, parameter.getClass()).newInstance(parameter);
    } catch (IllegalAccessException e) {
      throw new InstantiationException(e.getMessage());
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
        currentLogger().error("Unexpected exception in plugin cache cleaner", t);
      }
    }
  }
}
