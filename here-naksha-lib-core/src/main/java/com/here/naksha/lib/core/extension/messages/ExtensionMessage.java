package com.here.naksha.lib.core.extension.messages;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.util.json.JsonObject;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Base class of all Naksha extension protocol messages.
 */
@AvailableSince(INaksha.v2_0_3)
@JsonSubTypes({
  @JsonSubTypes.Type(value = ProcessEventMsg.class),
  @JsonSubTypes.Type(value = ResponseMsg.class),
  @JsonSubTypes.Type(value = SendUpstreamMsg.class)
})
public class ExtensionMessage extends JsonObject implements Typed {
  // Note: We may want in the future to allow Naksha extensions to invoke new tasks on the Naksha-Hub. For this
  // purpose we only
  //       new to create a new message type, the result will anyway still be a ResponseMsg.
}
