package com.here.xyz.models.hub.view;

import com.here.xyz.models.hub.ProcessorParams;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ViewProcessorParams extends ProcessorParams {

  protected ViewProcessorParams(@NotNull Map<@NotNull String, @Nullable Object> connectorParams, @NotNull String logId) {
    super(logId);

  }

  // TODO: Do we any parameters at all?
}
