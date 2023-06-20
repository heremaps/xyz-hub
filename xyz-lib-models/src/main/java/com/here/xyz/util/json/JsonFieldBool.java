package com.here.xyz.util.json;


import com.here.xyz.util.Unsafe;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class JsonFieldBool<OBJECT> extends JsonField<OBJECT, Boolean> {

    JsonFieldBool(
            @NotNull JsonClass<OBJECT> jsonClass,
            @NotNull Field javaField,
            int index,
            @NotNull String jsonName,
            @Nullable String defaultValue) {
        super(jsonClass, javaField, index, jsonName, defaultValue);
        this.defaultValue = "true".equalsIgnoreCase(defaultValue);
    }

    @Override
    public @NotNull Boolean defaultValue() {
        return defaultValue;
    }

    @Override
    public @NotNull Boolean nullValue() {
        return Boolean.FALSE;
    }

    @Override
    public @NotNull Boolean value(@Nullable Object value) {
        if (value instanceof Boolean v) {
            return v;
        }
        if (value == null) {
            return nullValue();
        }
        throw new IllegalArgumentException("value is no instance of " + valueClass.getName());
    }

    @Override
    public @NotNull Boolean _get(@NotNull OBJECT object) {
        return Unsafe.unsafe.getBoolean(object, offset);
    }

    @Override
    public void _put(@NotNull OBJECT object, Boolean value) {
        assert value != null;
        Unsafe.unsafe.putBoolean(object, offset, value);
    }

    @Override
    public boolean _compareAndSwap(@NotNull OBJECT object, Boolean expected, Boolean value) {
        assert expected != null && value != null;
        final int byteNumber = (int) (this.offset & 3);
        final int BITS;
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
            // Value of byte #0 is in the lower bits.
            BITS = byteNumber << 3;
        } else {
            // Value of byte #0 is in the higher bits.
            BITS = 24 - (byteNumber << 3);
        }
        final int UNMASK = ~(0xff << BITS);
        // Align offset to 4 byte boundary.
        final long offset = this.offset & 0xffff_ffff_ffff_fffCL;
        while (true) {
            final int current = Unsafe.unsafe.getInt(object, offset);
            final boolean current_value = ((current >>> BITS) & 0xff) != 0;
            if (current_value != expected) {
                return false;
            }
            final int new_value = (current & UNMASK) | ((value ? 1 << BITS : 0) << BITS);
            if (Unsafe.unsafe.compareAndSwapInt(object, offset, current, new_value)) {
                return true;
            }
            // We need to loop, because possibly some code modified bytes we're not interested in.
        }
    }

    private final boolean defaultValue;
}
