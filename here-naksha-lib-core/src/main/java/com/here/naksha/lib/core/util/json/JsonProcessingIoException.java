package com.here.naksha.lib.core.util.json;


import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JsonProcessingIoException extends JsonProcessingException {

    public JsonProcessingIoException(@Nullable Throwable rootCause) {
        super(rootCause.getMessage(), null, rootCause);
    }

    public JsonProcessingIoException(@NotNull String msg) {
        super(msg, null, null);
    }

    public JsonProcessingIoException(@NotNull String msg, @Nullable Throwable rootCause) {
        super(msg, null, rootCause);
    }

    public JsonProcessingIoException(@NotNull String msg, @Nullable JsonLocation loc, @Nullable Throwable rootCause) {
        super(msg, loc, rootCause);
    }
}
