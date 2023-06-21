package com.here.naksha.lib.core.util.diff;

import java.util.ArrayList;

/** Represents a difference between two lists. */
public class ListDiff extends ArrayList<Difference> implements Difference {

    int originalLength;
    int newLength;

    ListDiff(final int totalLength) {
        super(totalLength);
    }
}
