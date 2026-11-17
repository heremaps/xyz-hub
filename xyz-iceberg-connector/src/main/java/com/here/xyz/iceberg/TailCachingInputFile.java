/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * Serves the Parquet footer of a file from memory.
 * The tail of a Parquet file is {@code <footer><footer length (4 bytes, little endian)>PAR1}. On the first read in the last 8 bytes,
 * the stream reads the footer length, then the footer, and caches both by file location (data files are immutable).
 * Later streams of the same file serve every read inside that tail from the cache. All other reads go to the wrapped file, which
 * is opened lazily.
 */
final class TailCachingInputFile implements InputFile {

  private static final int TRAILER_LENGTH = 8;
  private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

  private final InputFile delegate;
  private final Cache<String, byte[]> tailCache;

  TailCachingInputFile(InputFile delegate, Cache<String, byte[]> tailCache) {
    this.delegate = delegate;
    this.tailCache = tailCache;
  }

  @Override
  public long getLength() {
    return delegate.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    return new TailCachingStream();
  }

  @Override
  public String location() {
    return delegate.location();
  }

  @Override
  public boolean exists() {
    return delegate.exists();
  }

  private final class TailCachingStream extends SeekableInputStream {

    private final long length = getLength();
    private long pos;
    private byte[] tail;
    private long tailStart;
    private boolean tailChecked;
    private SeekableInputStream stream;

    private TailCachingStream() {
      byte[] cached = tailCache.getIfPresent(location());
      if (cached != null)
        useTail(cached);
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0)
        throw new EOFException("Cannot seek to a negative position " + newPos);
      pos = newPos;
    }

    @Override
    public int read() throws IOException {
      byte[] single = new byte[1];
      int n = read(single, 0, 1);
      return n <= 0 ? -1 : single[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0)
        return 0;
      if (pos >= length)
        return -1;
      if (!tailChecked && pos >= length - TRAILER_LENGTH)
        loadTail();
      if (tail != null && pos >= tailStart) {
        int n = (int) Math.min(len, length - pos);
        System.arraycopy(tail, (int) (pos - tailStart), b, off, n);
        pos += n;
        return n;
      }
      SeekableInputStream s = stream();
      if (s.getPos() != pos)
        s.seek(pos);
      int n = s.read(b, off, len);
      if (n > 0)
        pos += n;
      return n;
    }

    @Override
    public void close() throws IOException {
      if (stream != null)
        stream.close();
    }

    private void useTail(byte[] cached) {
      tail = cached;
      tailStart = length - cached.length;
      tailChecked = true;
    }

    private void loadTail() throws IOException {
      tailChecked = true;
      if (length < TRAILER_LENGTH + MAGIC.length)
        return;
      byte[] trailer = readFully(length - TRAILER_LENGTH, TRAILER_LENGTH);
      if (!Arrays.equals(MAGIC, 0, MAGIC.length, trailer, 4, TRAILER_LENGTH))
        return;
      int footerLength = ByteBuffer.wrap(trailer, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      if (footerLength < 0 || (long) footerLength + TRAILER_LENGTH > length)
        return;
      byte[] loaded = new byte[footerLength + TRAILER_LENGTH];
      byte[] footer = readFully(length - loaded.length, footerLength);
      System.arraycopy(footer, 0, loaded, 0, footerLength);
      System.arraycopy(trailer, 0, loaded, footerLength, TRAILER_LENGTH);
      tailCache.put(location(), loaded);
      useTail(loaded);
    }

    private byte[] readFully(long position, int n) throws IOException {
      SeekableInputStream s = stream();
      s.seek(position);
      byte[] buffer = new byte[n];
      int read = 0;
      while (read < n) {
        int r = s.read(buffer, read, n - read);
        if (r < 0)
          throw new EOFException("Unexpected end of " + location() + " at " + (position + read));
        read += r;
      }
      return buffer;
    }

    private SeekableInputStream stream() {
      if (stream == null)
        stream = delegate.newStream();
      return stream;
    }
  }
}
