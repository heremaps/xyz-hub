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

import static com.here.xyz.responses.XyzError.TIMEOUT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * Reads byte ranges of data files in parallel, each with one bounded read.
 * Ranges of a file which are less than {@link #MAX_GAP} apart are merged into one read, up to {@link #MAX_MERGED_LENGTH}.
 * On S3, a stream of Iceberg's {@code S3FileIO} is {@link RangeReadable}, so a range is one GET with a byte range, not the
 * open-ended GET of a sequential stream.
 */
final class RangeReader {

  static final long MAX_GAP = 256 * 1024;
  /**
   * Merged ranges stay below this length (a single range can be longer), so that large reads still run in parallel
   */
  static final long MAX_MERGED_LENGTH = 8L * 1024 * 1024;

  private final ExecutorService pool;

  RangeReader(ExecutorService pool) {
    this.pool = pool;
  }

  /**
   * The byte ranges to read from one file.
   *
   * @param file The file, not counting its reads (the bytes count against the budget before the reads)
   * @param ranges {offset, length} pairs; they may overlap and be in any order
   */
  record FileRanges(InputFile file, List<long[]> ranges) {}

  /**
   * Reads the ranges of one file, see {@link #fetch(List, ReadBudget, FeatureScan.Stats)}.
   *
   * @return The bytes read by the offset of each merged range
   */
  Map<Long, byte[]> fetch(InputFile file, List<long[]> ranges, ReadBudget budget, FeatureScan.Stats stats) {
    return fetch(List.of(new FileRanges(file, ranges)), budget, stats).get(0);
  }

  /**
   * Reads the ranges of the files in parallel. All bytes count against the budget before the first read starts, so a round that
   * does not fit fails before it reads anything.
   *
   * @return For each file, the bytes read by the offset of each merged range
   * @throws ScanException with PAYLOAD_TO_LARGE if the bytes do not fit into the budget, with TIMEOUT if the reads did not
   *     finish before the deadline of the budget
   */
  List<Map<Long, byte[]>> fetch(List<FileRanges> files, ReadBudget budget, FeatureScan.Stats stats) {
    List<List<long[]>> merged = new ArrayList<>();
    long total = 0;
    for (FileRanges file : files) {
      List<long[]> fileRanges = merge(file.ranges());
      merged.add(fileRanges);
      for (long[] range : fileRanges)
        total += range[1];
    }
    budget.reserve(total);

    List<Callable<byte[]>> reads = new ArrayList<>();
    for (int i = 0; i < files.size(); i++) {
      InputFile file = files.get(i).file();
      for (long[] range : merged.get(i))
        reads.add(() -> read(file, range[0], (int) range[1]));
    }
    List<byte[]> results = invokeAll(reads, budget.deadlineMillis());
    stats.rangeReads += reads.size();

    List<Map<Long, byte[]>> fetched = new ArrayList<>();
    int next = 0;
    for (List<long[]> fileRanges : merged) {
      Map<Long, byte[]> bytes = new TreeMap<>();
      for (long[] range : fileRanges)
        bytes.put(range[0], results.get(next++));
      fetched.add(bytes);
    }
    return fetched;
  }

  /**
   * Runs the jobs on the pool and waits for all of them until the deadline. The jobs must not wait for other jobs of the pool.
   *
   * @throws ScanException with TIMEOUT if not all jobs finished before the deadline; the others are cancelled then
   */
  <T> List<T> invokeAll(List<Callable<T>> jobs, long deadlineMillis) {
    List<Future<T>> futures = new ArrayList<>();
    for (Callable<T> job : jobs)
      futures.add(pool.submit(job));
    List<T> results = new ArrayList<>();
    try {
      for (Future<T> future : futures) {
        long wait = deadlineMillis - System.currentTimeMillis();
        if (wait <= 0)
          throw new TimeoutException();
        results.add(future.get(wait, MILLISECONDS));
      }
      return results;
    }
    catch (TimeoutException e) {
      futures.forEach(future -> future.cancel(true));
      throw new ScanException(TIMEOUT, "The Iceberg read did not finish in time");
    }
    catch (InterruptedException e) {
      futures.forEach(future -> future.cancel(true));
      Thread.currentThread().interrupt();
      throw new ScanException(TIMEOUT, "The Iceberg read was interrupted");
    }
    catch (ExecutionException e) {
      futures.forEach(future -> future.cancel(true));
      if (e.getCause() instanceof RuntimeException runtimeException)
        throw runtimeException;
      if (e.getCause() instanceof IOException ioException)
        throw new UncheckedIOException(ioException);
      throw new IllegalStateException(e.getCause());
    }
  }

  /**
   * Reads one range of a file in the calling thread, with a bounded read if the stream supports it.
   */
  byte[] read(InputFile file, long offset, int length) {
    byte[] buffer = new byte[length];
    try (SeekableInputStream stream = file.newStream()) {
      if (stream instanceof RangeReadable rangeReadable)
        rangeReadable.readFully(offset, buffer, 0, length);
      else {
        stream.seek(offset);
        int read = 0;
        while (read < length) {
          int n = stream.read(buffer, read, length - read);
          if (n < 0)
            throw new EOFException("Unexpected end of " + file.location() + " at " + (offset + read));
          read += n;
        }
      }
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return buffer;
  }

  /**
   * Sorts the ranges and merges the ones which overlap or are less than {@link #MAX_GAP} apart, as long as the merged range
   * stays below {@link #MAX_MERGED_LENGTH}.
   *
   * @return {offset, length} pairs, sorted by offset
   */
  static List<long[]> merge(List<long[]> ranges) {
    List<long[]> sorted = new ArrayList<>(ranges);
    sorted.sort(Comparator.comparingLong(range -> range[0]));
    List<long[]> merged = new ArrayList<>();
    long start = -1, end = -1;
    for (long[] range : sorted) {
      if (range[1] <= 0)
        continue;
      long rangeEnd = range[0] + range[1];
      if (start >= 0 && range[0] <= end + MAX_GAP && (rangeEnd <= end || Math.max(end, rangeEnd) - start <= MAX_MERGED_LENGTH))
        end = Math.max(end, rangeEnd);
      else {
        if (start >= 0)
          merged.add(new long[] {start, end - start});
        //The part of an overlapping range which is not read yet
        start = start >= 0 ? Math.max(range[0], end) : range[0];
        end = rangeEnd;
      }
    }
    if (start >= 0)
      merged.add(new long[] {start, end - start});
    for (long[] range : merged)
      if (range[1] > Integer.MAX_VALUE)
        throw new IllegalStateException("A range of " + range[1] + " bytes cannot be read at once");
    return merged;
  }
}
