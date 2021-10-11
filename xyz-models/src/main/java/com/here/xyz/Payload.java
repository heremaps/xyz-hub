/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.ByteStreams;
import com.here.xyz.events.Event;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.Hasher;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;

@JsonSubTypes({
    @JsonSubTypes.Type(value = Event.class),
    @JsonSubTypes.Type(value = XyzResponse.class)
})
public class Payload implements Typed {

  public static final String VERSION = "0.6.0";

  public static InputStream prepareInputStream(InputStream input) throws IOException {
    if (!input.markSupported()) {
      input = new BufferedInputStream(input);
    }

    if (isCompressed(input)) {
      input = gunzip(input);
    }

    return input;
  }

  @SuppressWarnings("WeakerAccess")
  public static InputStream gunzip(InputStream is) throws IOException {
    try {
      return new BufferedInputStream(new GZIPInputStream(is));
    } catch (ZipException z) {
      return is;
    }
  }

  public static OutputStream gzip(OutputStream os) throws IOException {
    try {
      return new GZIPOutputStream(os);
    } catch (ZipException z) {
      return os;
    }
  }

  /**
   * Determines if a byte array is compressed. The java.util.zip GZip implementation does not expose the GZip header so it is difficult to
   * determine if a string is compressed.
   *
   * @param is an input stream
   * @return true if the array is compressed or false otherwise
   */
  public static boolean isCompressed(InputStream is) {
    try {
      if (!is.markSupported()) {
        is = new BufferedInputStream(is);
      }

      byte[] bytes = new byte[2];
      is.mark(2);
      is.read(bytes);
      is.reset();
      return isGzipped(bytes);
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isGzipped(byte[] bytes) {
    return bytes != null && bytes.length >= 2 && GZIPInputStream.GZIP_MAGIC == (((int) bytes[0] & 0xff) | ((bytes[1] << 8) & 0xff00));
  }

  public static byte[] compress(byte[] bytes) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
      gos.write(bytes);
    } catch (IOException e) {
      return null;
    }

    return baos.toByteArray();
  }

  public static byte[] decompress(final byte[] bytes) throws IOException {
    return ByteStreams.toByteArray(Payload.prepareInputStream(new ByteArrayInputStream(bytes)));
  }

  /**
   * @param versionA
   * @param versionB
   * @return -1 if versionA is smaller than versionB, 0 if versionA equals versionB, 1 if versionA is larger than versionB.
   */
  public static int compareVersions(String versionA, String versionB) {
    String[] partsA = versionA.split(".");
    String[] partsB = versionB.split(".");
    if (partsA.length != partsB.length) throw new IllegalArgumentException("Incompatible version strings.");
    for (int i = 0; i < partsA.length; i++) {
      int versionPartA = Integer.parseInt(partsA[i]), versionPartB = Integer.parseInt(partsB[i]);
      if (versionPartA < versionPartB)
        return -1;
      else if (versionPartA > versionPartB)
        return 1;
    }
    return 0;
  }

  /**
   * Returns the hash of the event as a base64 string.
   */
  @JsonIgnore
  public String getHash() {
    try {
      return Hasher.getHash(getCacheString());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("WeakerAccess")
  @JsonIgnore
  public String getCacheString() throws JsonProcessingException {
    return SORTED_MAPPER.get()
        // Adding .writerWithView(Object.class) will serialize all properties, which do not have a view annotation. Any properties, which
        // are not used as an input to generate the response(e.g. the request log stream Id) and do not result in a change of the response
        // must be annotated with a view ( e.g. @JsonView(ExcludeFromHash.class) )
        .writerWithView(Object.class)
        .writeValueAsString(this);
  }

  @Override
  public String toString() {
    return serialize();
  }

  protected static class ExcludeFromHash {

  }
}
