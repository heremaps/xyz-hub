package com.here.naksha.lib.core.extension.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * Send by the extension to Naksha-Hub, when an extension is done with processing an event. Send as well by Naksha-Hub as response to a
 * {@link SendUpstreamMsg}.
 */
@AvailableSince(INaksha.v2_0_3)
@JsonTypeName(value = "naksha.ext.rpc.v1.returnResponse")
public class ResponseMsg extends ExtensionMessage {

  public static final String RESPONSE = "response";

  @AvailableSince(INaksha.v2_0_3)
  @JsonCreator
  public ResponseMsg(@JsonProperty(RESPONSE) @NotNull XyzResponse response) {
    this.response = response;
  }

  /**
   * The response to return.
   */
  @AvailableSince(INaksha.v2_0_3)
  @JsonProperty(RESPONSE)
  public final @NotNull XyzResponse response;
}
