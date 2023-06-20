package com.here.xyz.lambdas;

@FunctionalInterface
public interface Pe3<A, B, C> extends Pe {

    void call(A a, B b, C c) throws Exception;
}
