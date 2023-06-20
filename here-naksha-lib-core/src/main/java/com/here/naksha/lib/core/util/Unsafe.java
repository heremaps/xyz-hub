package com.here.naksha.lib.core.util;


import java.lang.reflect.Field;
import org.jetbrains.annotations.NotNull;

/** Grant access to the unsafe. */
public class Unsafe {

    /** The unsafe. */
    public static final @NotNull sun.misc.Unsafe unsafe;

    static {
        // http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/687fd7c7986d/src/share/classes/sun/misc/Unsafe.java
        try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            // mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
            // unmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
            f.setAccessible(true);
            unsafe = (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) {
            throw new InternalError(e);
        }
    }
}
