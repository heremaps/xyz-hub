package com.here.naksha.lib.core.util.json;


import com.here.naksha.lib.core.util.Unsafe;
import java.lang.reflect.Field;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unchecked")
public class JsonFieldObject<OBJECT, VALUE> extends JsonField<OBJECT, VALUE> {

    protected JsonFieldObject(
            @NotNull JsonClass<OBJECT> jsonClass,
            @NotNull Field javaField,
            int index,
            @NotNull String jsonName,
            @Nullable String defaultValue) {
        super(jsonClass, javaField, index, jsonName, defaultValue);
        this.nullValue = null;
        if (defaultValue != null && defaultValue.length() > 0) {
            if (!valueClass.isInstance(defaultValue)) {
                throw new InternalError("@JsonProperty annotation with defaultValue for incompatible type");
            }
            this.defaultValue = (VALUE) defaultValue;
        } else {
            this.defaultValue = null;
        }
    }

    protected VALUE defaultValue;
    protected VALUE nullValue;

    @Override
    public @Nullable VALUE defaultValue() {
        return defaultValue;
    }

    @Override
    public @Nullable VALUE nullValue() {
        return nullValue;
    }

    @Override
    public @Nullable VALUE value(@Nullable Object value) {
        if (value == null || valueClass.isInstance(value)) {
            return (VALUE) value;
        }
        throw new IllegalArgumentException("value is no instance of " + valueClass.getName());
    }

    @Override
    public @Nullable VALUE _get(@NotNull OBJECT object) {
        return (VALUE) Unsafe.unsafe.getObject(object, offset);
    }

    @Override
    public void _put(@NotNull OBJECT object, @Nullable VALUE value) {
        Unsafe.unsafe.putObject(object, offset, value);
    }

    @Override
    public boolean _compareAndSwap(@NotNull OBJECT object, @Nullable VALUE expected, @Nullable VALUE value) {
        return Unsafe.unsafe.compareAndSwapObject(object, offset, expected, value);
    }
}
