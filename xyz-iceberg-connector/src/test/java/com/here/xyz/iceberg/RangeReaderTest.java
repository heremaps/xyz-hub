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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.here.xyz.responses.XyzError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RangeReaderTest {

  private static final int FILE_LENGTH = 4 * 1024 * 1024;

  @TempDir
  static Path dir;
  private static byte[] content;
  private static InputFile file;
  private static ExecutorService pool;
  private static RangeReader reader;

  @BeforeAll
  static void writeFile() throws IOException {
    content = new byte[FILE_LENGTH];
    for (int i = 0; i < content.length; i++)
      content[i] = (byte) (i * 31 + (i >> 8));
    Path path = dir.resolve("data.bin");
    Files.write(path, content);
    file = org.apache.iceberg.Files.localInput(path.toFile());
    pool = Executors.newFixedThreadPool(4);
    reader = new RangeReader(pool);
  }

  @AfterAll
  static void stopPool() {
    pool.shutdownNow();
  }

  @Test
  void mergesRangesWithSmallGaps() {
    long gap = RangeReader.MAX_GAP;
    List<long[]> merged = RangeReader.merge(List.of(
        new long[] {1000, 100},
        new long[] {0, 100},
        new long[] {100 + gap, 50},
        new long[] {50, 10},
        new long[] {200 + 2 * gap, 10},
        new long[] {300 + 2 * gap, 0}));
    assertThat(merged).containsExactly(new long[] {0, 150 + gap}, new long[] {200 + 2 * gap, 10});
  }

  @Test
  void mergedRangesStayBelowTheMaximumLength() {
    long max = RangeReader.MAX_MERGED_LENGTH;
    List<long[]> merged = RangeReader.merge(List.of(new long[] {0, max - 10}, new long[] {max - 10, 100}, new long[] {max + 50, 10},
        new long[] {max + 200, 3 * max}));
    assertThat(merged).containsExactly(new long[] {0, max - 10}, new long[] {max - 10, 100}, new long[] {max + 200, 3 * max});
    //An overlapping range is split at the end of the previous one
    assertThat(RangeReader.merge(List.of(new long[] {0, max}, new long[] {max - 10, 100})))
        .containsExactly(new long[] {0, max}, new long[] {max, 90});
  }

  @Test
  void readsTheRangesInParallel() {
    FeatureScan.Stats stats = new FeatureScan.Stats();
    ReadBudget budget = new ReadBudget(Long.MAX_VALUE, Long.MAX_VALUE);
    Map<Long, byte[]> fetched = reader.fetch(file, List.of(new long[] {10, 20}, new long[] {2_000_000, 1000},
        new long[] {3_000_000, 5}), budget, stats);
    assertThat(fetched).containsOnlyKeys(10L, 2_000_000L, 3_000_000L);
    assertThat(fetched.get(10L)).isEqualTo(Arrays.copyOfRange(content, 10, 30));
    assertThat(fetched.get(2_000_000L)).isEqualTo(Arrays.copyOfRange(content, 2_000_000, 2_001_000));
    assertThat(stats.rangeReads).isEqualTo(3);
    assertThat(budget.bytesRead()).isEqualTo(1025);
  }

  @Test
  void theBudgetIsCountedBeforeTheReads() {
    AtomicInteger streams = new AtomicInteger();
    InputFile counted = new DelegatingFile(file) {
      @Override
      public SeekableInputStream newStream() {
        streams.incrementAndGet();
        return super.newStream();
      }
    };
    FeatureScan.Stats stats = new FeatureScan.Stats();
    ReadBudget budget = new ReadBudget(1000, Long.MAX_VALUE);
    assertThatThrownBy(() -> reader.fetch(counted, List.of(new long[] {0, 600}, new long[] {1_000_000, 600}), budget, stats))
        .isInstanceOfSatisfying(ScanException.class, e -> assertThat(e.error()).isEqualTo(XyzError.PAYLOAD_TO_LARGE));
    assertThat(streams.get()).isZero();
    assertThat(budget.bytesRead()).isZero();
    assertThat(stats.rangeReads).isZero();
  }

  @Test
  void stopsAtTheDeadline() {
    InputFile slow = new DelegatingFile(file) {
      @Override
      public SeekableInputStream newStream() {
        try {
          Thread.sleep(2_000);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return super.newStream();
      }
    };
    ReadBudget budget = new ReadBudget(Long.MAX_VALUE, System.currentTimeMillis() + 100);
    long start = System.currentTimeMillis();
    assertThatThrownBy(() -> reader.fetch(slow, List.of(new long[] {0, 10}, new long[] {1_000_000, 10}), budget,
        new FeatureScan.Stats()))
        .isInstanceOfSatisfying(ScanException.class, e -> assertThat(e.error()).isEqualTo(XyzError.TIMEOUT));
    assertThat(System.currentTimeMillis() - start).isLessThan(1_500);
  }

  @Test
  void aBufferedFileServesItsRangesAndReadsMissingBytes() throws IOException {
    FeatureScan.Stats stats = new FeatureScan.Stats();
    ReadBudget budget = new ReadBudget(Long.MAX_VALUE, Long.MAX_VALUE);
    BufferedParquetFile buffered = new BufferedParquetFile(file, reader, budget, stats);
    buffered.add(100, Arrays.copyOfRange(content, 100, 200));
    buffered.add(200, Arrays.copyOfRange(content, 200, 300));
    assertThat(buffered.getLength()).isEqualTo(FILE_LENGTH);

    try (org.apache.parquet.io.SeekableInputStream stream = buffered.newStream()) {
      byte[] bytes = new byte[150];
      stream.seek(120);
      stream.readFully(bytes);
      assertThat(bytes).isEqualTo(Arrays.copyOfRange(content, 120, 270));
      assertThat(stream.getPos()).isEqualTo(270);
      assertThat(stats.unplannedReads).isZero();
      assertThat(budget.bytesRead()).isZero();

      //A read across the end of the buffered bytes reads the missing ones once
      stream.seek(250);
      bytes = new byte[100];
      stream.readFully(bytes);
      assertThat(bytes).isEqualTo(Arrays.copyOfRange(content, 250, 350));
      assertThat(stats.unplannedReads).isEqualTo(1);
      assertThat(budget.bytesRead()).isEqualTo(50);
      stream.seek(300);
      ByteBuffer buffer = ByteBuffer.allocate(50);
      stream.readFully(buffer);
      assertThat(buffer.array()).isEqualTo(Arrays.copyOfRange(content, 300, 350));
      assertThat(stats.unplannedReads).isEqualTo(1);

      stream.seek(FILE_LENGTH - 1);
      assertThat(stream.read()).isEqualTo(content[FILE_LENGTH - 1] & 0xFF);
      assertThat(stream.read()).isEqualTo(-1);
    }
  }

  private static class DelegatingFile implements InputFile {

    private final InputFile delegate;

    DelegatingFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return delegate.newStream();
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }
}
