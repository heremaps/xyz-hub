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
    value = parseValueWithDefault(params, "value", 5L);
  }

  final @Nullable String foo;
  final long value;
}
/*

  connector
  {
    "eventHandler": "activityLog" // -> com.here.naksha.activitylog.ActivityLogHandler
    "params": {
      "foo": "Hello"
    }
  }

 */
