package com.here.naksha.lib.core.util.json;

import com.here.naksha.lib.core.util.Unsafe;
import java.lang.reflect.Field;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class JsonFieldInt<OBJECT> extends JsonField<OBJECT, Integer> {

    JsonFieldInt(
            @NotNull JsonClass<OBJECT> jsonClass,
            @NotNull Field javaField,
            int index,
            @NotNull String jsonName,
            @Nullable String defaultValue) {
        super(jsonClass, javaField, index, jsonName, defaultValue);
        this.nullValue = 0;
        this.defaultValue =
                defaultValue != null && defaultValue.length() > 0 ? Integer.parseInt(defaultValue) : nullValue;
    }

    @Override
    public @NotNull Integer defaultValue() {
        return defaultValue;
    }

    @Override
    public @NotNull Integer nullValue() {
        return nullValue;
    }

    @Override
    public @NotNull Integer value(@Nullable Object value) {
        if (value instanceof Integer v) {
            return v;
        }
        if (value == null) {
            return nullValue();
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        throw new IllegalArgumentException("value is no instance of " + valueClass.getName());
    }

    @Override
    public @NotNull Integer _get(@NotNull OBJECT object) {
        return Unsafe.unsafe.getInt(object, offset);
    }

    @Override
    public void _put(@NotNull OBJECT object, Integer value) {
        assert value != null;
        Unsafe.unsafe.putInt(object, offset, value);
    }

    @Override
    public boolean _compareAndSwap(@NotNull OBJECT object, Integer expected, Integer value) {
        assert expected != null && value != null;
        return Unsafe.unsafe.compareAndSwapInt(object, offset, expected, value);
    }

    private final Integer defaultValue;
    private final Integer nullValue;
}
