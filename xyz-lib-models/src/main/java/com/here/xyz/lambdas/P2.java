package com.here.xyz.lambdas;

@FunctionalInterface
public interface P2<A, B> extends P {

    void call(A a, B b);
}
