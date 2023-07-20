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
package com.here.xyz.hub.util;

import io.vertx.core.buffer.Buffer;
import java.io.IOException;
import java.io.InputStream;
import org.jetbrains.annotations.NotNull;

public class BufferInputStream extends InputStream {

  private final @NotNull Buffer buffer;
  private int position;

  public BufferInputStream(@NotNull Buffer buffer) {
    this.buffer = buffer;
    this.position = 0;
  }

  @Override
  public int read() throws IOException {
    if (position >= buffer.length()) {
      return -1; // End of stream reached
    }

    int value = buffer.getByte(position) & 0xFF;
    position++;
    return value;
  }

  @Override
  public int read(byte @NotNull [] b, int off, int len) throws IOException {
    if (position >= buffer.length()) {
      return -1; // End of stream reached
    }

    final int bytesToRead = Math.min(len, buffer.length() - position);
    buffer.getBytes(position, position + bytesToRead, b, off);
    position += bytesToRead;
    return bytesToRead;
  }
}
