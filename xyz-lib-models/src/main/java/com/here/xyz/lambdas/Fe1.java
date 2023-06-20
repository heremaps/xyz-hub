package com.here.xyz.lambdas;

@FunctionalInterface
public interface Fe1<Z, A> extends Fe {

    Z call(A a) throws Exception;
}
