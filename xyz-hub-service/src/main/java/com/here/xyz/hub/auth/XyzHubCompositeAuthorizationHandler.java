package com.here.xyz.hub.auth;

import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;

public class XyzHubCompositeAuthorizationHandler implements CompositeAuthorizationHandler {

  public Future<Boolean> authorize(RoutingContext context) {
    return Future.succeededFuture(true);
  }
}
