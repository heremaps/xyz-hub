package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface F2<Z, A, B> extends F {

  Z call(A a, B b);
}
