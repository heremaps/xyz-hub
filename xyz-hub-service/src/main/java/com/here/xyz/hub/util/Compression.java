/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class Compression {
  /**
   * Decompress a byte array which was compressed using Deflate.
   * @param bytearray non-null byte array to be decompressed
   * @return the decompressed payload or an empty array in case of bytearray is null
   * @throws DataFormatException in case the payload cannot be decompressed
   */
  public static byte[] decompressUsingInflate(byte[] bytearray) throws DataFormatException {
    if (bytearray == null) return new byte[0];

    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      final Inflater inflater = new Inflater();
      final byte[] buff = new byte[1024];

      inflater.setInput(bytearray);

      while (!inflater.finished()) {
        int count = inflater.inflate(buff);
        bos.write(buff, 0, count);
      }

      inflater.end();
      bos.flush();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new DataFormatException(e.getMessage());
    }
  }

  /**
   *
   * @param bytearray
   * @return
   * @throws DataFormatException
   */
  public static byte[] compressUsingInflate(byte[] bytearray) throws DataFormatException {
    if (bytearray == null) return new byte[0];

    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      final Deflater deflater = new Deflater();
      final byte[] buff = new byte[1024];

      deflater.setInput(bytearray);
      deflater.finish();

      while (!deflater.finished()) {
        int count = deflater.deflate(buff);
        bos.write(buff, 0, count);
      }

      deflater.end();
      bos.flush();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new DataFormatException(e.getMessage());
    }
  }
}
