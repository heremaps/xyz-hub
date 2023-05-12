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
