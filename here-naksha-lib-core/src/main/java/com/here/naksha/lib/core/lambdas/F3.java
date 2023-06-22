package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface F3<Z, A, B, C> extends F {

  Z call(A a, B b, C c);
}
