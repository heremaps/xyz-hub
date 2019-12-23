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

import java.util.zip.DataFormatException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class CompressionTest {

  @Test
  public void testDeflateInflate() throws DataFormatException {
    final StringBuilder a = new StringBuilder();

    for (int i=0; i<1000; i++) {
      a.append("isaudhciu q4789hf9eriu f9q8e47h f9ehq49 sadiuh iusadhiu adisuhiu adisiuhasdiuhiu fasd dfdf");
    }

    final String originalString = a.toString();
    final byte[] originalByteArray = originalString.getBytes();
    final byte[] compressedBytearray = Compression.compressUsingInflate(originalByteArray);
    final byte[] resultingBytearray = Compression.decompressUsingInflate(compressedBytearray);
    final String resultingString = new String(resultingBytearray);

    Assert.assertTrue(originalByteArray.length >= resultingBytearray.length);
    Assert.assertTrue(StringUtils.equals(originalString, resultingString));
  }
}
