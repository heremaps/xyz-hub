package com.here.xyz.hub.spi;

import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;

public interface AuthorizationHandler {
  Future<Boolean> authorize(RoutingContext context);
}
