package com.here.xyz.hub.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public abstract class ApiUnitTest {

  private static Method findDeclaredMethod(Class<?> cls, String name, Class<?>[] paramTypes) throws NoSuchMethodException {
    Class<?> c = cls;
    while (c != null) {
      try {
        return c.getDeclaredMethod(name, paramTypes);
      } catch (NoSuchMethodException ignored) {
        c = c.getSuperclass();
      }
    }
    throw new NoSuchMethodException("Method '" + name + "' not found on class " + cls.getName());
  }

  protected static RoutingContext ctxWithQuery(String query) {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    HttpServerRequest req = ctx.request();
    when(req.query()).thenReturn(query);
    when(ctx.get(anyString())).thenAnswer(inv -> null);
    when(ctx.put(anyString(), any())).thenAnswer(inv -> ctx);
    return ctx;
  }

  protected static RoutingContext ctxWithPathParam(String name, String value) {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(ctx.pathParam(name)).thenReturn(value);
    when(ctx.request().query()).thenReturn(null);
    when(ctx.get(anyString())).thenReturn(null);
    when(ctx.put(anyString(), any())).thenReturn(ctx);
    return ctx;
  }

  protected Object invokePrivate(Object target, String name, Class<?>[] paramTypes, Object... args) {
    try {
      Method m = findDeclaredMethod(target.getClass(), name, paramTypes);
      m.setAccessible(true);
      return m.invoke(target, args);
    } catch (InvocationTargetException e) {
      if (e.getTargetException() instanceof RuntimeException) {
        throw (RuntimeException) e.getTargetException();
      }
      if (e.getTargetException() instanceof Error) {
        throw (Error) e.getTargetException();
      }
      throw new RuntimeException(e.getTargetException());
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
