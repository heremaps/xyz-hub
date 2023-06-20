package com.here.xyz.util.diff;

/**
 * An exception thrown if applying a patch fails, the creation of a difference fails or any other
 * merge error occurs.
 */
public class MergeConflictException extends Exception {

    MergeConflictException(String msg) {
        super(msg);
    }
}
