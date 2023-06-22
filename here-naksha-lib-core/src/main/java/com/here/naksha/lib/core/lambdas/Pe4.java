package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface Pe4<A, B, C, D> extends Pe {

  void call(A a, B b, C c, D d) throws Exception;
}
