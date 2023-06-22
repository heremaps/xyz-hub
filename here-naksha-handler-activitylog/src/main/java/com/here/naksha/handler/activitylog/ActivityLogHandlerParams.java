package com.here.naksha.handler.activitylog;

import com.here.naksha.lib.core.EventHandlerParams;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The parameter parser. */
class ActivityLogHandlerParams extends EventHandlerParams {

  ActivityLogHandlerParams(@NotNull Map<@NotNull String, @Nullable Object> params) {
    // param1 - storage_mode can have DIFF_ONLY or FULL or FEATURE_ONLY. Default will be FULL
    storage_mode = parseValueWithDefault(params, "storage_mode", "FULL");
    // param2 - (Optional) Keeps a maximum of x states per object (x > 0).
    states = parseValueWithDefault(params, "states", 1);
    // param3 - (Optional) Write and update the invalidateAt timestamp. If set to false, this can
    // reduce space requests.
    writeInvalidatedAt = parseValueWithDefault(params, "writeInvalidatedAt", false);
  }

  final String storage_mode;
  final int states;
  final boolean writeInvalidatedAt;
}
/*

connector
{
"eventHandler": "activityLog" // -> com.here.naksha.activitylog.ActivityLogHandler
"params": {
"storage_mode": "DIFF_ONLY",
"states":5
}
}

*/
