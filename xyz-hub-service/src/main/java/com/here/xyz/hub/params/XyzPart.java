package com.here.xyz.hub.params;

import java.io.Serializable;

/**
 * The part to return of a result-set. Virtually split the result-set into {@link #total} parts and returns the part number {@link #part}
 * with {@link #part} being a value between {@code 1} and {@link #total}.
 */
public class XyzPart implements Serializable {

  public XyzPart(int part, int total) {
    this.part = part;
    this.total = total;
  }

  /**
   * The part to return, must be greater than zero and less than or equal {@link #total}.
   */
  public final int part;

  /**
   * The total amount of parts into which to split a result-set.
   */
  public final int total;

}
