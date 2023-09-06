package com.here.xyz.hub.spi;

import io.vertx.ext.web.RoutingContext;

public interface AuthorizationHandler {
  boolean authorize(RoutingContext context);
}
