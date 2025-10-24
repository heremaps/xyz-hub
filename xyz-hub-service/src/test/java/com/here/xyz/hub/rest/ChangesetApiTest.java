package com.here.xyz.hub.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

import com.here.xyz.models.hub.Ref;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import org.junit.Before;
import org.junit.Test;

public class ChangesetApiTest extends ApiUnitTest {

  private ChangesetApi api;

  @Before
  public void setUp() {
    RouterBuilder rb = mock(RouterBuilder.class, RETURNS_DEEP_STUBS);
    api = new ChangesetApi(rb);
  }

  @Test
  public void buildIterateChangesetsEvent_timeValidation() {
    RoutingContext ctx1 = ctxWithQuery("startTime=-5");
    IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate(api, "buildIterateChangesetsEvent", new Class<?>[]{RoutingContext.class, Ref.class},
            ctx1,
            new Ref(Ref.HEAD)));
    assertEquals("The parameter \"startTime\" must be >= 0.", ex1.getMessage());

    RoutingContext ctx2 = ctxWithQuery("endTime=-1");
    IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate(api, "buildIterateChangesetsEvent", new Class<?>[]{RoutingContext.class, Ref.class},
            ctx2,
            new Ref(Ref.HEAD)));
    assertEquals("The parameter \"endTime\" must be >= 0.", ex2.getMessage());

    RoutingContext ctx3 = ctxWithQuery("startTime=10&endTime=5");
    IllegalArgumentException ex3 = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate(api, "buildIterateChangesetsEvent", new Class<?>[]{RoutingContext.class, Ref.class},
            ctx3,
            new Ref(Ref.HEAD)));
    assertEquals("The parameter \"startTime\" needs to be smaller than or equal to \"endTime\".", ex3.getMessage());
  }

  @Test
  public void getChangesets_rangeValidation_startGreaterThanEnd() {
    RoutingContext ctx = ctxWithQuery("startVersion=5&endVersion=3");
    assertThrows(RuntimeException.class,
        () -> invokePrivate(api, "getChangesets", new Class<?>[]{RoutingContext.class}, ctx));
  }

  @Test
  public void getLongQueryParam_throwsOnNegative() {
    RoutingContext ctx = ctxWithQuery("startVersion=-1");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate(api, "getLongQueryParam", new Class<?>[]{RoutingContext.class, String.class, long.class}, ctx, "startVersion",
            0L));
    assertEquals("The parameter \"startVersion\" must be >= 0.", ex.getMessage());
  }

  @Test
  public void getLongQueryParam_throwsOnNonNumber() {
    RoutingContext ctx = ctxWithQuery("endVersion=abc");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate(api, "getLongQueryParam", new Class<?>[]{RoutingContext.class, String.class, long.class}, ctx, "endVersion",
            -1L));
    assertEquals("The parameter \"endVersion\" is not a number.", ex.getMessage());
  }

  @Test
  public void getVersionFromPathParam_required() {
    RoutingContext ctx = ctxWithPathParam(ApiParam.Path.VERSION, null);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate(api, "getVersionFromPathParam", new Class<?>[]{RoutingContext.class}, ctx));
    assertEquals("The parameter \"version\" is required.", ex.getMessage());
  }

  @Test
  public void getVersionFromPathParam_nonNumeric() {
    RoutingContext ctx = ctxWithPathParam(ApiParam.Path.VERSION, "abc");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> invokePrivate(api, "getVersionFromPathParam", new Class<?>[]{RoutingContext.class}, ctx));
    String expectedPrefix = "The parameter \"version\" is not a number.";
    assertEquals(expectedPrefix, ex.getMessage());
  }
}
