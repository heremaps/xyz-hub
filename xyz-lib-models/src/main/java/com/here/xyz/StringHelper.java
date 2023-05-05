package com.here.xyz;

import org.jetbrains.annotations.NotNull;

public final class StringHelper {

  public static boolean equals(@NotNull CharSequence a, @NotNull CharSequence b) {
    return equals(a, 0, a.length(), b, 0, b.length());
  }

  public static boolean equals(
      @NotNull CharSequence a,
      int aStart,
      int aEnd,
      @NotNull CharSequence b,
      int bStart,
      int bEnd) {
    if (aStart > aEnd || aStart < 0 || aStart > a.length() || aEnd > a.length()) return false;
    if (bStart > bEnd || bStart < 0 || bStart > b.length() || aEnd > b.length()) return false;
    if ((aEnd - aStart) != (bEnd - bStart)) return false;
    while (aStart < aEnd) {
      final char ac = a.charAt(aStart++);
      final char bc = b.charAt(bStart++);
      if (ac != bc) return false;
    }
    return true;
  }
}
