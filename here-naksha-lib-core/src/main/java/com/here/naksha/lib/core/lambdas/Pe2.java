package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface Pe2<A, B> extends Pe {

  void call(A a, B b) throws Exception;
}
