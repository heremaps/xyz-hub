package com.here.xyz.hub.spi;

import io.vertx.ext.web.Router;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.List;
import java.util.Map;

public interface RestConfiguration {
  byte[] getContract();

  List<String> getPublicApis();

  String getServicePrefix();

  String getDataContainerUrlTemplate();

  Map<String, String> getDataContainerPathParams();

  String getSpaceIdTemplate();

  void processRouterBuilder(RouterBuilder routerBuilder);

  void processAdditionalRoutes(Router router);
}
