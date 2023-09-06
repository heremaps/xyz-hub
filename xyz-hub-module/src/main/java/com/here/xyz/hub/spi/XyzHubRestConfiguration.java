package com.here.xyz.hub.spi;

import io.vertx.ext.web.Router;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XyzHubRestConfiguration implements RestConfiguration {

  @Override
  public byte[] getContract() {
    return new byte[0];
  }

  @Override
  public List<String> getPublicApis() {
    return Arrays.asList("contract", "stable", "experimental");
  }

  @Override
  public String getServicePrefix() {
    return "/hub";
  }

  @Override
  public String getDataContainerUrlTemplate() {
    return "/spaces/{spaceId}";
  }

  @Override
  public Map<String, String> getDataContainerPathParams() {
    return new HashMap<>() {{
      put("spaceId", "{spaceId}");
    }};
  }

  @Override
  public String getSpaceIdTemplate() {
    return "{spaceId}";
  }

  @Override
  public void processRouterBuilder(RouterBuilder routerBuilder) {

  }

  @Override
  public void processAdditionalRoutes(Router router) {

  }
}
