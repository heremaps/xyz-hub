package com.here.xyz.lambdas;

@FunctionalInterface
public interface F4<Z, A, B, C, D> extends F {

    Z call(A a, B b, C c, D d);
}
