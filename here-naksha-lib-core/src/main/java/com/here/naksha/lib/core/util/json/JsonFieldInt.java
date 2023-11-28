/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
    if (value instanceof Integer) {
      return (Integer) value;
    }
    if (value == null) {
      return nullValue();
    }
    if (value instanceof Number) {
      Number n = (Number) value;
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
