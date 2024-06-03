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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class JsonFieldLong<OBJECT> extends JsonField<OBJECT, Long> {

  JsonFieldLong(
      @NotNull JsonClass<OBJECT> jsonClass,
      @NotNull Field javaField,
      int index,
      @NotNull String jsonName,
      @Nullable String defaultValue) {
    super(jsonClass, javaField, index, jsonName, defaultValue);
    this.nullValue = 0L;
    this.defaultValue =
        defaultValue != null && defaultValue.length() > 0 ? Long.parseLong(defaultValue) : nullValue;
  }

  @Override
  public @NotNull Long defaultValue() {
    return defaultValue;
  }

  @Override
  public @NotNull Long nullValue() {
    return nullValue;
  }

  @Override
  public @NotNull Long value(@Nullable Object value) {
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value == null) {
      return nullValue();
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    throw new IllegalArgumentException("value is no instance of " + valueClass.getName());
  }

  @Override
  public @NotNull Long _get(@NotNull OBJECT object) {
    return Unsafe.unsafe.getLong(object, offset);
  }

  @Override
  public void _put(@NotNull OBJECT object, Long value) {
    assert value != null;
    Unsafe.unsafe.putLong(object, offset, value);
  }

  @Override
  public boolean _compareAndSwap(@NotNull OBJECT object, Long expected, Long value) {
    assert expected != null && value != null;
    return Unsafe.unsafe.compareAndSwapLong(object, offset, expected, value);
  }

  private final Long defaultValue;
  private final Long nullValue;
}
