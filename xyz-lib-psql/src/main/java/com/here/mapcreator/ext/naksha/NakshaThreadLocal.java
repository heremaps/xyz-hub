package com.here.mapcreator.ext.naksha;

import java.lang.ref.WeakReference;
import org.jetbrains.annotations.NotNull;

public final class NakshaThreadLocal {

  private NakshaThreadLocal() {
  }

  private static final class NakshaLocalWeakRef extends WeakReference<NakshaThreadLocal> {

    public NakshaLocalWeakRef(@NotNull NakshaThreadLocal referent) {
      super(referent);
    }
  }

  private static final class NakshaThreadLocalImpl extends ThreadLocal<@NotNull NakshaLocalWeakRef> {

    @Override
    protected NakshaLocalWeakRef initialValue() {
      return new NakshaLocalWeakRef(new NakshaThreadLocal());
    }

    @NotNull NakshaThreadLocal update() {
      final NakshaThreadLocal local = new NakshaThreadLocal();
      final NakshaLocalWeakRef ref = new NakshaLocalWeakRef(local);
      set(ref);
      return local;
    }
  }

  private static final NakshaThreadLocalImpl threadLocal = new NakshaThreadLocalImpl();

  // -----------------------------------------------------------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------------------------------------------------------
  // -----------------------------------------------------------------------------------------------------------------------------------

  /**
   * Returns the current thread local Naksha local.
   *
   * @return the current thread local Naksha local.
   */
  public static @NotNull NakshaThreadLocal get() {
    @NotNull NakshaLocalWeakRef ref = threadLocal.get();
    NakshaThreadLocal local = ref.get();
    return local != null ? local : threadLocal.update();
  }

  private final @NotNull StringBuilder sb = new StringBuilder();

  /**
   * A thread local string builder. Use with care, do not use this and call another method that does use it as well!
   *
   * @return the string builder with length set to 0.
   */
  public @NotNull StringBuilder sb() {
    sb.setLength(0);
    return sb;
  }
}