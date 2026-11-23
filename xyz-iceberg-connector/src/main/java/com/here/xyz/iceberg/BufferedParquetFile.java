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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * A Parquet file whose reads are served from byte ranges in memory: the cached footer and page index and the pages fetched by
 * the {@link RangeReader}. A read outside of them (which the planning of the reader did not foresee) reads the missing bytes
 * synchronously with one bounded read, counts them against the budget and counts an unplanned read. It never fails because of a
 * missing range.
 * <p>
 * An instance is used by one thread at a time.
 */
final class BufferedParquetFile implements org.apache.parquet.io.InputFile {

  private final InputFile file;
  private final long length;
  private final RangeReader rangeReader;
  private final ReadBudget budget;
  private final FeatureScan.Stats stats;
  private final NavigableMap<Long, byte[]> ranges = new TreeMap<>();

  /**
   * @param file The data file, not counting its reads
   */
  BufferedParquetFile(InputFile file, RangeReader rangeReader, ReadBudget budget, FeatureScan.Stats stats) {
    this.file = file;
    length = file.getLength();
    this.rangeReader = rangeReader;
    this.budget = budget;
    this.stats = stats;
  }

  /**
   * Adds bytes of the file, starting at the offset.
   */
  void add(long offset, byte[] bytes) {
    if (bytes != null && bytes.length > 0)
      ranges.merge(offset, bytes, (a, b) -> a.length >= b.length ? a : b);
  }

  void addAll(Map<Long, byte[]> fetched) {
    fetched.forEach(this::add);
  }

  /**
   * Drops the buffered bytes of the ranges, so that the memory of a round can be reclaimed.
   *
   * @param offsets The offsets of the ranges, as passed to {@link #add(long, byte[])}
   */
  void removeAll(List<Long> offsets) {
    offsets.forEach(ranges::remove);
  }

  String location() {
    return file.location();
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new BufferedStream();
  }

  /**
   * Copies bytes of the file into the array, reading the missing ones.
   */
  private void copy(long position, byte[] target, int offset, int count) throws IOException {
    if (position < 0 || position + count > length)
      throw new EOFException("Cannot read " + count + " bytes at " + position + " of " + file.location() + " (" + length + " bytes)");
    while (count > 0) {
      Entry<Long, byte[]> range = ranges.floorEntry(position);
      if (range != null && position < range.getKey() + range.getValue().length) {
        int from = (int) (position - range.getKey());
        int n = Math.min(count, range.getValue().length - from);
        System.arraycopy(range.getValue(), from, target, offset, n);
        position += n;
        offset += n;
        count -= n;
        continue;
      }
      Long next = ranges.higherKey(position);
      int missing = (int) (next == null ? count : Math.min(count, next - position));
      budget.reserve(missing);
      stats.unplannedReads++;
      add(position, rangeReader.read(file, position, missing));
    }
  }

  private final class BufferedStream extends SeekableInputStream {

    private long pos;

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
      if (pos >= length)
        return -1;
      byte[] single = new byte[1];
      readFully(single, 0, 1);
      return single[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0)
        return 0;
      if (pos >= length)
        return -1;
      int n = (int) Math.min(len, length - pos);
      readFully(b, off, n);
      return n;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      copy(pos, bytes, start, len);
      pos += len;
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      if (!buf.hasRemaining())
        return 0;
      if (pos >= length)
        return -1;
      int n = (int) Math.min(buf.remaining(), length - pos);
      readInto(buf, n);
      return n;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      readInto(buf, buf.remaining());
    }

    private void readInto(ByteBuffer buf, int n) throws IOException {
      if (buf.hasArray()) {
        readFully(buf.array(), buf.arrayOffset() + buf.position(), n);
        buf.position(buf.position() + n);
      }
      else {
        byte[] bytes = new byte[n];
        readFully(bytes, 0, n);
        buf.put(bytes);
      }
    }
  }
}
