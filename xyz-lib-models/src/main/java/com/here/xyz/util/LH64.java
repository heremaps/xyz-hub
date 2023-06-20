package com.here.xyz.util;


import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Helper class to combine two 32-bit values into one 64-bit value.
 *
 * @since 2.0.0
 */
@AvailableSince("2.0.0")
public final class LH64 {

    /**
     * Combine the low and high 32-bit integers to one 64-bit integer.
     *
     * @param low The lower 32-bit value.
     * @param high The higher 32-bit value.
     * @return The 64-bit value.
     */
    @AvailableSince("2.0.0")
    public static long lh64(int low, int high) {
        return ((high & 0xffffffffL) << 32) | (low & 0xffffffffL);
    }

    /**
     * Extract the higher 32-bit value.
     *
     * @param lh The combined 64-bit value.
     * @return The higher 32-bit value.
     */
    @AvailableSince("2.0.0")
    public static int highInt(long lh) {
        return (int) (lh >>> 32);
    }

    /**
     * Extract the lower 32-bit value.
     *
     * @param lh The combined 64-bit value.
     * @return The lower 32-bit value.
     */
    @AvailableSince("2.0.0")
    public static int lowInt(long lh) {
        return (int) (lh & 0xffffffffL);
    }
}
