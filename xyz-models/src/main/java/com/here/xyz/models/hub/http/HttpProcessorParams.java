package com.here.xyz.models.hub.http;

import com.here.xyz.models.hub.ProcessorParams;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HttpProcessorParams extends ProcessorParams {

  protected HttpProcessorParams(@NotNull Map<@NotNull String, @Nullable Object> connectorParams, @NotNull String logId) {
    super(logId);

  }

  // TODO: Add parameters like URL, connectTimeout, readTimeout
}
