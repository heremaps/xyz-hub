package com.here.xyz.hub.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.junit.Before;
import org.junit.Test;

public class ChangesetApiTest {

  private ChangesetApi api;

  private static RoutingContext ctxWithQuery(String query) {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    HttpServerRequest req = ctx.request();
    when(req.query()).thenReturn(query);
    when(ctx.get(anyString())).thenAnswer(inv -> null);
    when(ctx.put(anyString(), any())).thenAnswer(inv -> ctx);
    return ctx;
  }

  private static RoutingContext ctxWithPathParam(String name, String value) {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(ctx.pathParam(name)).thenReturn(value);
    when(ctx.request().query()).thenReturn(null);
    when(ctx.get(anyString())).thenReturn(null);
    when(ctx.put(anyString(), any())).thenReturn(ctx);
    return ctx;
  }

  @Before
  public void setUp() {
    RouterBuilder rb = mock(RouterBuilder.class, RETURNS_DEEP_STUBS);
    api = new ChangesetApi(rb);
  }

  private Object invokePrivate(String name, Class<?>[] paramTypes, Object... args) {
    try {
      Method m = MethodHandles.lookup()
          .in(ChangesetApi.class)
          .lookupClass()
          .getDeclaredMethod(name, paramTypes);
      m.setAccessible(true);
      return m.invoke(api, args);
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

  @Test
  public void buildIterateChangesetsEvent_timeValidation() {
    RoutingContext ctx1 = ctxWithQuery("startTime=-5");
    IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate("buildIterateChangesetsEvent", new Class<?>[]{RoutingContext.class, com.here.xyz.models.hub.Ref.class}, ctx1,
            new com.here.xyz.models.hub.Ref(com.here.xyz.models.hub.Ref.HEAD)));
    assertEquals("The parameter \"startTime\" must be >= 0.", ex1.getMessage());

    RoutingContext ctx2 = ctxWithQuery("endTime=-1");
    IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate("buildIterateChangesetsEvent", new Class<?>[]{RoutingContext.class, com.here.xyz.models.hub.Ref.class}, ctx2,
            new com.here.xyz.models.hub.Ref(com.here.xyz.models.hub.Ref.HEAD)));
    assertEquals("The parameter \"endTime\" must be >= 0.", ex2.getMessage());

    RoutingContext ctx3 = ctxWithQuery("startTime=10&endTime=5");
    IllegalArgumentException ex3 = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate("buildIterateChangesetsEvent", new Class<?>[]{RoutingContext.class, com.here.xyz.models.hub.Ref.class}, ctx3,
            new com.here.xyz.models.hub.Ref(com.here.xyz.models.hub.Ref.HEAD)));
    assertEquals("The parameter \"startTime\" needs to be smaller than or equal to \"endTime\".", ex3.getMessage());
  }

  @Test
  public void getChangesets_rangeValidation_startGreaterThanEnd() {
    RoutingContext ctx = ctxWithQuery("startVersion=5&endVersion=3");
    assertThrows(RuntimeException.class,
        () -> invokePrivate("getChangesets", new Class<?>[]{RoutingContext.class}, ctx));
  }

  @Test
  public void getLongQueryParam_throwsOnNegative() {
    RoutingContext ctx = ctxWithQuery("startVersion=-1");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate("getLongQueryParam", new Class<?>[]{RoutingContext.class, String.class, long.class}, ctx, "startVersion", 0L));
    assertEquals("The parameter \"startVersion\" must be >= 0.", ex.getMessage());
  }

  @Test
  public void getLongQueryParam_throwsOnNonNumber() {
    RoutingContext ctx = ctxWithQuery("endVersion=abc");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate("getLongQueryParam", new Class<?>[]{RoutingContext.class, String.class, long.class}, ctx, "endVersion", -1L));
    assertEquals("The parameter \"endVersion\" is not a number.", ex.getMessage());
  }

  @Test
  public void getVersionFromPathParam_required() {
    RoutingContext ctx = ctxWithPathParam(ApiParam.Path.VERSION, null);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate("getVersionFromPathParam", new Class<?>[]{RoutingContext.class}, ctx));
    assertEquals("The parameter \"version\" is required.", ex.getMessage());
  }

  @Test
  public void getVersionFromPathParam_nonNumeric() {
    RoutingContext ctx = ctxWithPathParam(ApiParam.Path.VERSION, "abc");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate("getVersionFromPathParam", new Class<?>[]{RoutingContext.class}, ctx));
    String expectedPrefix = "The parameter \"version\" is not a number.";
    assertEquals(expectedPrefix, ex.getMessage());
  }
}
