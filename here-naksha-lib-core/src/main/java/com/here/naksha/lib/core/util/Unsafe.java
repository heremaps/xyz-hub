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
package com.here.naksha.lib.core.util;

import java.lang.reflect.Field;
import org.jetbrains.annotations.NotNull;

/**
 * Grant access to the unsafe.
 */
public class Unsafe {

  /**
   * The unsafe.
   */
  public static final @NotNull sun.misc.Unsafe unsafe;

  static {
    // http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/687fd7c7986d/src/share/classes/sun/misc/Unsafe.java
    try {
      Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      // mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
      // unmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
      f.setAccessible(true);
      unsafe = (sun.misc.Unsafe) f.get(null);
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }

  /**
   * Returns the offset of the field with the given name.
   * @param objectClass the class to query.
   * @param fieldName the name of the field to query.
   * @return the offset.
   */
  public static long fieldOffset(@NotNull Class<?> objectClass, @NotNull String fieldName) {
    final Field field;
    try {
      field = objectClass.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      throw new Error("No such field: " + objectClass.getName() + "::" + fieldName, e);
    }
    return unsafe.objectFieldOffset(field);
  }
}
