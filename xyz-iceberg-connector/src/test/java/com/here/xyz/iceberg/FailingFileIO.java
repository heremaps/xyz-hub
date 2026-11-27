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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A local FileIO whose input streams fail with an S3 error of the status {@link #STATUS} while {@link #FAILURES} is positive, one
 * failure per opened stream. Set it as {@code io-impl} of a catalog to test how reads handle storage errors.
 */
public class FailingFileIO implements FileIO {

  /**
   * The number of streams which fail before streams open again
   */
  static final AtomicInteger FAILURES = new AtomicInteger();
  /**
   * The HTTP status code of the failures
   */
  static volatile int STATUS = 503;
  private final HadoopFileIO delegate = new HadoopFileIO(new Configuration());

  public FailingFileIO() {}

  /**
   * Disarms the failures and restores the default status.
   */
  static void reset() {
    FAILURES.set(0);
    STATUS = 503;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public InputFile newInputFile(String path) {
    return new FailingInputFile(delegate.newInputFile(path));
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return new FailingInputFile(delegate.newInputFile(path, length));
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(path);
  }

  @Override
  public void close() {
    delegate.close();
  }

  private record FailingInputFile(InputFile delegate) implements InputFile {

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      if (FAILURES.getAndDecrement() > 0)
        throw S3Exception.builder().statusCode(STATUS).message("test failure").build();
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
