package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface P4<A, B, C, D> extends P {

  void call(A a, B b, C c, D d);
}
