package com.here.xyz.hub.spi;

import com.here.xyz.hub.auth.CompositeAuthorizationHandler;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class Modules {
  private static final Logger logger = LogManager.getLogger();
  private static final CompositeAuthorizationHandler authorizationHandlerInstance = load(CompositeAuthorizationHandler.class);


  public static CompositeAuthorizationHandler getCompositeAuthorizationHandler() {
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
