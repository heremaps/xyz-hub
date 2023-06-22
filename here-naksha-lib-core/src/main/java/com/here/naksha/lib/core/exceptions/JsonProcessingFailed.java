package com.here.naksha.lib.core.exceptions;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

/**
 * A wrapper class to throw an {@link JsonProcessingException} or an {@link IOException} as runtime exception.
 */
public class JsonProcessingFailed extends RuntimeException {

  public JsonProcessingFailed(@NotNull IOException e) {
    super(e);
  }

}
