package com.here.xyz.util.diff;

/**
 * A common interface for primitive changes.
 */
public abstract class PrimitiveDiff implements Difference {

  private final Object newValue;
  private final Object oldValue;

  public PrimitiveDiff(Object oldValue, Object newValue) {
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  /**
   * Returns the old value.
   *
   * @return the old value.
   */
  public Object oldValue() {
    return oldValue;
  }

  /**
   * Returns the new value.
   *
   * @return the new value.
   */
  public Object newValue() {
    return newValue;
  }
}
