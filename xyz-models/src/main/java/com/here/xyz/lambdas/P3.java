package com.here.xyz.lambdas;

@FunctionalInterface
public interface P3<A, B, C> extends P {

  void call(A a, B b, C c);
}
