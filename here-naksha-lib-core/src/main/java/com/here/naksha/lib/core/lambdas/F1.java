package com.here.naksha.lib.core.lambdas;

@FunctionalInterface
public interface F1<Z, A> extends F {

    Z call(A a);
}
