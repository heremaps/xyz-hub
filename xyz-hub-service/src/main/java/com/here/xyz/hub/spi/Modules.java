package com.here.xyz.hub.spi;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class Modules {
  private static final Logger logger = LogManager.getLogger();
  private static final RestConfiguration restConfigurationInstance = load(RestConfiguration.class);
  private static final AuthorizationHandler authorizationHandlerInstance = load(AuthorizationHandler.class);

  public static RestConfiguration getRestConfiguration() {
    return restConfigurationInstance;
  }

  public static AuthorizationHandler getAuthorizationHandler() {
    return authorizationHandlerInstance;
  }

  private static <T> T load(Class<T> klass) {
    ServiceLoader<T> loader = ServiceLoader.load(klass);
    Optional<Provider<T>> optionalProvider = loader.stream().findFirst();

    if (optionalProvider.isEmpty()) {
      logger.fatal("Module not present: " + klass);
      throw new RuntimeException();
    }

    return optionalProvider.get().get();
  }
}
