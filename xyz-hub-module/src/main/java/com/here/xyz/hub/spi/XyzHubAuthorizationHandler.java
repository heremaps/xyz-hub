package com.here.xyz.hub.spi;

import io.vertx.ext.web.RoutingContext;

public class XyzHubAuthorizationHandler implements AuthorizationHandler {

  public boolean authorize(RoutingContext context) {
    return true;
  }
}
