package com.here.xyz.lambdas;

@FunctionalInterface
public interface F1<Z, A> extends F {

    Z call(A a);
}
