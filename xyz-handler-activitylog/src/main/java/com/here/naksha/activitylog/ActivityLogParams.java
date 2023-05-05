package com.here.naksha.activitylog;

import com.here.xyz.EventHandlerParams;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The parameter parser.
 */
class ActivityLogParams extends EventHandlerParams {

  ActivityLogParams(@NotNull Map<@NotNull String, @Nullable Object> params) {
    foo = parseOptionalValue(params, "foo", String.class);
  }

  final @Nullable String foo;
}
