package com.here.xyz.hub.auth;

import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;

public interface CompositeAuthorizationHandler {
  Future<Boolean> authorize(RoutingContext context);
}
