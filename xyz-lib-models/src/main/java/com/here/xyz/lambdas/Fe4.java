package com.here.xyz.lambdas;

@FunctionalInterface
public interface Fe4<Z, A, B, C, D> extends Fe {

    Z call(A a, B b, C c, D d) throws Exception;
}