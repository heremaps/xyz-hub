package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface P2<A, B> extends P {

  void call(A a, B b);
}
