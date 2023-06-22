package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface P5<A, B, C, D, E> extends P {

  void call(A a, B b, C c, D d, E e);
}
