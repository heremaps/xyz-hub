package com.here.naksha.lib.core.extension.messages;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.util.json.JsonObject;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

@AvailableSince(INaksha.v2_0_3)
@JsonSubTypes({
    @JsonSubTypes.Type(value = ProcessEvent.class),
    @JsonSubTypes.Type(value = ReturnResponse.class),
    @JsonSubTypes.Type(value = SendUpstream.class)
})
public class ExtensionMessage extends JsonObject implements Typed {}
