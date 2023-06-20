package com.here.xyz.util;


import java.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A string builder that works like the default {@link StringBuilder}, except that it supports
 * UNICODE code points.
 */
public class UnicodeBuilder {

  public UnicodeBuilder() {
    this(16);
  }

  public UnicodeBuilder(int initial_capacity) {
    this.codePoints = new int[Math.max(2, initial_capacity)];
  }

  protected int @NotNull [] codePoints;
  protected int end;
  protected char @Nullable [] chars;
  protected @Nullable String string;

  protected void modified() {
    string = null;
  }

  protected void ensureCapacity(int capacity) {
    if (codePoints.length < capacity) {
      codePoints = Arrays.copyOf(codePoints, capacity + 16);
    }
  }

  /**
   * Set the new length
   *
   * @param length The new length.
   */
  public void setLength(int length) {
    final int new_end = Math.max(0, length);
    if (new_end == end) {
      return;
    }
    if (new_end < end) {
      end = new_end;
    } else { // new_end > end
      ensureCapacity(new_end);
      final int[] codePoints = this.codePoints;
      for (int i = end; i < new_end; i++) {
        // codePoints[i] =
      }
    }
    modified();
  }

  /**
   * Returns the amount of code points in the builder.
   *
   * @return the amount of code points in the builder.
   */
  public int length() {
    return end;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return o == this;
  }

  @Override
  public @NotNull String toString() {
    if (string != null) {
      return string;
    }
    if (chars == null || chars.length < (end << 1)) {
      chars = new char[codePoints.length << 1];
    }
    int c = 0;
    for (int i = 0; i < end; i++) {
      final int unicode = codePoints[i] & 0x001f_ffff;
      if (Character.isBmpCodePoint(unicode)) {
        chars[c++] = (char) unicode;
      } else {
        chars[c++] = Character.highSurrogate(unicode);
        chars[c++] = Character.lowSurrogate(unicode);
      }
    }
    string = new String(chars, 0, c);
    return string;
  }
}
