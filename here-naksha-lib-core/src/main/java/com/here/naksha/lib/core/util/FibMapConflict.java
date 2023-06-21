package com.here.naksha.lib.core.util;

import com.here.naksha.lib.core.lambdas.F1;
import com.here.naksha.lib.core.lambdas.F4;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The object to return, when a {@link FibMap#put(Object, Object, Object, boolean, Object[])} fails,
 * because the current value is not the expected. The constructor ({@code FibMapConflict::new}) can
 * be passed as last argument to {@link FibMap#put(Object, Object, Object, boolean, Object[], F1,
 * F4)}.
 *
 * @param key The key, that should be modified.
 * @param expected_value The value expected.
 * @param new_value The value to be set.
 * @param value The value found (will not the expected).
 */
public record FibMapConflict(
        @NotNull Object key, @Nullable Object expected_value, @Nullable Object new_value, @Nullable Object value) {}
