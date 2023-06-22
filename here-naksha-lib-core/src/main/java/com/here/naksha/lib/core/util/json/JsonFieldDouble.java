package com.here.naksha.lib.core.util.json;

import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.longBitsToDouble;

import com.here.naksha.lib.core.util.Unsafe;
import java.lang.reflect.Field;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class JsonFieldDouble<OBJECT> extends JsonField<OBJECT, Double> {

  JsonFieldDouble(
      @NotNull JsonClass<OBJECT> jsonClass,
      @NotNull Field javaField,
      int index,
      @NotNull String jsonName,
      @Nullable String defaultValue) {
    super(jsonClass, javaField, index, jsonName, defaultValue);
    this.nullValue = 0d;
    this.defaultValue =
        defaultValue != null && defaultValue.length() > 0 ? Double.parseDouble(defaultValue) : nullValue;
  }

  @Override
  public @NotNull Double defaultValue() {
    return defaultValue;
  }

  @Override
  public @NotNull Double nullValue() {
    return nullValue;
  }

  @Override
  public @NotNull Double value(@Nullable Object value) {
    if (value instanceof Double v) {
      return v;
    }
    if (value == null) {
      return nullValue();
    }
    if (value instanceof Number n) {
      return n.doubleValue();
    }
    throw new IllegalArgumentException("value is no instance of " + valueClass.getName());
  }

  @Override
  public @NotNull Double _get(@NotNull OBJECT object) {
    return longBitsToDouble(Unsafe.unsafe.getLong(object, offset));
  }

  @Override
  public void _put(@NotNull OBJECT object, Double value) {
    assert value != null;
    Unsafe.unsafe.putLong(object, offset, doubleToRawLongBits(value));
  }

  @Override
  public boolean _compareAndSwap(@NotNull OBJECT object, Double expected, Double value) {
    assert expected != null && value != null;
    return Unsafe.unsafe.compareAndSwapLong(
        object, offset, doubleToRawLongBits(expected), doubleToRawLongBits(value));
  }

  private final Double defaultValue;
  private final Double nullValue;
}
