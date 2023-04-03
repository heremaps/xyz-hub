package com.here.xyz.lambdas;

@FunctionalInterface
public interface P4<A, B, C, D> extends P {

  void call(A a, B b, C c, D d);
}
