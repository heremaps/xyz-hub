package com.here.mapcreator.ext.naksha;

import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;

/**
 * Just an abstraction for PSQL extension versioning.
 *
 * @param major    the major version (0-65535).
 * @param minor    the minor version (0-65535).
 * @param revision the revision (0-65535).
 */
public record PsqlExtVersion(int major, int minor, int revision) implements Comparable<PsqlExtVersion> {

  public PsqlExtVersion(long value) {
    this((int) ((value >>> 32) & 0xffff), (int) ((value >>> 16) & 0xffff), (int) (value & 0xffff));
  }

  public long toLong() {
    return (major & 0xffffL << 32) | ((minor & 0xffffL) << 16) | (revision & 0xffffL);
  }

  @Override
  public int compareTo(@NotNull PsqlExtVersion o) {
    final long result = toLong() - o.toLong();
    return result < 0 ? -1 : result == 0 ? 0 : 1;
  }

  @Override
  public int hashCode() {
    return (int) (toLong() >>> 16);
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof PsqlExtVersion o) {
      return this.toLong() == o.toLong();
    }
    return false;
  }
}