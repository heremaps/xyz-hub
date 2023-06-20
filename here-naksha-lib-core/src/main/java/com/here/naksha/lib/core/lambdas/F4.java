package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface F4<Z, A, B, C, D> extends F {

    Z call(A a, B b, C c, D d);
}
