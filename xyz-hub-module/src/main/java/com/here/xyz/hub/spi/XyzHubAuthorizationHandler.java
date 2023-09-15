package com.here.xyz.hub.spi;

import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;

public class XyzHubAuthorizationHandler implements AuthorizationHandler {

  public Future<Boolean> authorize(RoutingContext context) {
    return Future.succeededFuture(true);
  }
}
