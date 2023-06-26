/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.models.payload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.ByteStreams;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.util.Hasher;
import com.here.naksha.lib.core.util.json.JsonObject;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonSubTypes({@JsonSubTypes.Type(value = Event.class), @JsonSubTypes.Type(value = XyzResponse.class)})
public class Payload extends JsonObject implements Typed {

  protected static final Logger logger = LoggerFactory.getLogger(Payload.class);

  public static @NotNull InputStream prepareInputStream(@NotNull InputStream input) throws IOException {
    if (!input.markSupported()) {
      input = new BufferedInputStream(input);
    }
    if (isCompressed(input)) {
      input = gunzip(input);
    }
    return input;
  }

  @SuppressWarnings("WeakerAccess")
  public static @NotNull InputStream gunzip(@NotNull InputStream is) throws IOException {
    try {
      return new BufferedInputStream(new GZIPInputStream(is));
    } catch (ZipException z) {
      return is;
    }
  }

  public static @NotNull OutputStream gzip(@NotNull OutputStream os) throws IOException {
    try {
      return new GZIPOutputStream(os);
    } catch (ZipException z) {
      return os;
    }
  }

  /**
   * Determines if a byte array is compressed. The java.util.zip GZip implementation does not expose
   * the GZip header so it is difficult to determine if a string is compressed.
   *
   * @param is an input stream
   * @return true if the array is compressed or false otherwise
   */
  public static boolean isCompressed(@NotNull InputStream is) {
    try {
      if (!is.markSupported()) {
        is = new BufferedInputStream(is);
      }
      final byte[] bytes = new byte[2];
      is.mark(2);
      if (is.read(bytes) < bytes.length) {
        return false;
      }
      is.reset();
      return isGzipped(bytes);
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isGzipped(byte @NotNull [] bytes) {
    return bytes.length >= 2
        && GZIPInputStream.GZIP_MAGIC == (((int) bytes[0] & 0xff) | ((bytes[1] << 8) & 0xff00));
  }

  public static byte @NotNull [] compress(byte @NotNull [] bytes) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final GZIPOutputStream gos = new GZIPOutputStream(baos)) {
      gos.write(bytes);
      gos.flush();
      baos.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      logger.error("Unexpected exception while trying to GZIP given bytes", e);
      return bytes;
    }
  }

  public static byte @NotNull [] decompress(final byte @NotNull [] bytes) throws IOException {
    return ByteStreams.toByteArray(Payload.prepareInputStream(new ByteArrayInputStream(bytes)));
  }

  /**
   * @param versionA
   * @param versionB
   * @return -1 if versionA is smaller than versionB, 0 if versionA equals versionB, 1 if versionA
   *     is larger than versionB.
   */
  public static int compareVersions(String versionA, String versionB) {
    String[] partsA = versionA.split(".");
    String[] partsB = versionB.split(".");
    if (partsA.length != partsB.length) {
      throw new IllegalArgumentException("Incompatible version strings.");
    }
    for (int i = 0; i < partsA.length; i++) {
      int versionPartA = Integer.parseInt(partsA[i]), versionPartB = Integer.parseInt(partsB[i]);
      if (versionPartA < versionPartB) {
        return -1;
      } else if (versionPartA > versionPartB) {
        return 1;
      }
    }
    return 0;
  }

  /** Returns the hash of the event as a base64 string. */
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
  @Deprecated
  public String getCacheString() throws JsonProcessingException {
    return JsonSerializable.serialize(this);
  }

  @Override
  public String toString() {
    return serialize();
  }

  protected static class ExcludeFromHash {}
}
