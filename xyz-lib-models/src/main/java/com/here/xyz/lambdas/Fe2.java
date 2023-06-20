package com.here.xyz.lambdas;

@FunctionalInterface
public interface Fe2<Z, A, B> extends Fe {

    Z call(A a, B b) throws Exception;
}
