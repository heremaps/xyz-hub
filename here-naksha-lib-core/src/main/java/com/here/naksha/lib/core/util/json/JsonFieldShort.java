/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.core.util.json;

import com.here.naksha.lib.core.util.Unsafe;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class JsonFieldShort<OBJECT> extends JsonField<OBJECT, Short> {

  JsonFieldShort(
      @NotNull JsonClass<OBJECT> jsonClass,
      @NotNull Field javaField,
      int index,
      @NotNull String jsonName,
      @Nullable String defaultValue) {
    super(jsonClass, javaField, index, jsonName, defaultValue);
    this.nullValue = (short) 0;
    this.defaultValue =
        defaultValue != null && defaultValue.length() > 0 ? Short.parseShort(defaultValue) : nullValue;
  }

  @Override
  public @NotNull Short defaultValue() {
    return defaultValue;
  }

  @Override
  public @NotNull Short nullValue() {
    return nullValue;
  }

  @Override
  public @NotNull Short value(@Nullable Object value) {
    if (value instanceof Short) {
      return (Short) value;
    }
    if (value == null) {
      return nullValue();
    }
    if (value instanceof Number) {
      Number n = (Number) value;
      return n.shortValue();
    }
    throw new IllegalArgumentException("value is no instance of " + valueClass.getName());
  }

  @Override
  public @NotNull Short _get(@NotNull OBJECT object) {
    return Unsafe.unsafe.getShort(object, offset);
  }

  @Override
  public void _put(@NotNull OBJECT object, Short value) {
    assert value != null;
    Unsafe.unsafe.putShort(object, offset, value);
  }

  @Override
  public boolean _compareAndSwap(@NotNull OBJECT object, Short expected, Short value) {
    assert expected != null && value != null;
    final int byteNumber = (int) (this.offset & 3);
    final int BITS;
    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      // Value of byte #0 is in the lower bits.
      assert byteNumber == 0 || byteNumber == 2;
      BITS = byteNumber << 3;
    } else {
      // Value of byte #0 is in the higher bits.
      assert byteNumber == 0 || byteNumber == 2;
      BITS = 16 - (byteNumber << 3);
    }
    final int UNMASK = ~(0xffff << BITS);
    // Align offset to 4 byte boundary.
    final long offset = this.offset & 0xffff_ffff_ffff_fffCL;
    while (true) {
      final int current = Unsafe.unsafe.getInt(object, offset);
      final short current_value = (short) ((current >>> BITS) & 0xffff);
      if (current_value != expected) {
        return false;
      }
      final int new_value = (current & UNMASK) | ((value & 0xffff) << BITS);
      if (Unsafe.unsafe.compareAndSwapInt(object, offset, current, new_value)) {
        return true;
      }
      // We need to loop, because possibly some code modified bytes we're not interested in.
    }
  }

  private final Short defaultValue;
  private final Short nullValue;
}
