package com.here.xyz.util.diff;

/** An update. */
public class UpdateOp extends PrimitiveDiff {

  UpdateOp(Object oldValue, Object newValue) {
    super(oldValue, newValue);
  }
}
