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

import java.io.IOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * Counts the bytes actually read from the wrapped file against the {@link ReadBudget} of the request.
 */
final class CountingInputFile implements InputFile {

  private final InputFile delegate;
  private final ReadBudget budget;

  CountingInputFile(InputFile delegate, ReadBudget budget) {
    this.delegate = delegate;
    this.budget = budget;
  }

  @Override
  public long getLength() {
    return delegate.getLength();
  }

  @Override
  public SeekableInputStream newStream() {
    return new CountingStream(delegate.newStream(), budget);
  }

  @Override
  public String location() {
    return delegate.location();
  }

  @Override
  public boolean exists() {
    return delegate.exists();
  }

  private static final class CountingStream extends SeekableInputStream {

    private final SeekableInputStream delegate;
    private final ReadBudget budget;

    private CountingStream(SeekableInputStream delegate, ReadBudget budget) {
      this.delegate = delegate;
      this.budget = budget;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      int b = delegate.read();
      if (b >= 0)
        budget.addBytesRead(1);
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = delegate.read(b, off, len);
      if (n > 0)
        budget.addBytesRead(n);
      return n;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
