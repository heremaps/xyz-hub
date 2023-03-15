package com.here.xyz.models.hub;

import com.here.xyz.models.hub.psql.PsqlProcessorParams;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class recommended to implement processor parameters.
 */
@SuppressWarnings({"SameParameterValue", "unchecked"})
public abstract class ProcessorParams {

  protected static final Logger logger = LoggerFactory.getLogger(PsqlProcessorParams.class);

  protected ProcessorParams(@NotNull String logId) {
    this.logId = logId;
  }

  /**
   * The logging id, which should be placed at the first place of any log, like: <pre>{@code
   * logger.info("{} - Some Message", logId);
   * }</pre>
   */
  protected final @NotNull String logId;

  /**
   * Parses the given value from the connector parameters using the default value, if the given value is of an invalid type or not given.
   *
   * @param connectorParams The connector params.
   * @param name            The name of the parameter.
   * @param expectedType    The type that is expected.
   * @param <T>             The type of the value.
   * @return the value.
   * @throws NullPointerException     If the parameter is not given.
   * @throws IllegalArgumentException If the parameter is not of the expected type.
   */
  protected <T> @NotNull T parseValue(
      @NotNull Map<@NotNull String, @Nullable Object> connectorParams,
      @NotNull String name,
      @NotNull Class<T> expectedType
  ) {
    return parseValue(connectorParams, name, expectedType, null);
  }

  /**
   * Parses the given value from the connector parameters using the default value, if the given value is of an invalid type or not given.
   *
   * @param connectorParams the connector params.
   * @param name            the name of the parameter.
   * @param defaultValue    the default value to be used.
   * @param <T>             the type of the value.
   * @return the value.
   */
  protected <T> @NotNull T parseValue(
      @NotNull Map<@NotNull String, @Nullable Object> connectorParams,
      @NotNull String name,
      @NotNull T defaultValue
  ) throws NullPointerException {
    return parseValue(connectorParams, name, (Class<T>) defaultValue.getClass(), defaultValue);
  }

  private <T> @NotNull T parseValue(
      @NotNull Map<@NotNull String, @Nullable Object> connectorParams,
      @NotNull String parameter,
      @NotNull Class<T> type,
      @Nullable T defaultValue
  ) throws NullPointerException {
    final Object value = connectorParams.get(parameter);
    if (value == null) {
      if (defaultValue == null) {
        throw new NullPointerException(parameter);
      }
      return defaultValue;
    }
    if (value.getClass() == type) {
      return type.cast(value);
    }

    if (value instanceof Number) {
      if (type == Byte.class) {
        return (T) Byte.valueOf((((Number) value).byteValue()));
      } else if (type == Short.class) {
        return (T) Short.valueOf((((Number) value).shortValue()));
      } else if (type == Integer.class) {
        return (T) Integer.valueOf((((Number) value).intValue()));
      } else if (type == Long.class) {
        return (T) Long.valueOf((((Number) value).longValue()));
      } else if (type == Float.class) {
        return (T) Float.valueOf((((Number) value).floatValue()));
      } else if (type == Double.class) {
        return (T) Double.valueOf((((Number) value).doubleValue()));
      }
    }

    logger.warn("{} - Cannot set value {}={}. Load default '{}'", logId, parameter, value, defaultValue);
    if (defaultValue == null) {
      throw new IllegalArgumentException(parameter);
    }
    return defaultValue;
  }

}
