package com.here.xyz.httpconnector.util;

import io.vertx.core.Future;

/**
 * Some utility methods to work with Futures.
 */
public class Futures {

  public static <F> Future<F> futurify(ThrowingMapper<F> mapper) {
    try {
      return Future.succeededFuture(mapper.map());
    }
    catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @FunctionalInterface
  public interface ThrowingMapper<F> {
    F map() throws Exception;
  }
}
