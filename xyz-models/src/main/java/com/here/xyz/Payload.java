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

package com.here.xyz;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.Event;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.Hasher;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonSubTypes({
    @JsonSubTypes.Type(value = Event.class),
    @JsonSubTypes.Type(value = XyzResponse.class)
})
public class Payload implements Typed {

  private static final Logger logger = LoggerFactory.getLogger(Event.class);
  @JsonIgnore
  private static final ThreadLocal<MessageDigest> sha256 = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      logger.error("An unexpected NoSuchAlgorithmException ", e);
      return null;
    }
  });

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
   * @throws java.io.IOException if the byte array couldn't be read
   */
  public static boolean isCompressed(InputStream is) throws IOException {
    try {
      if (!is.markSupported()) {
        is = new BufferedInputStream(is);
      }

      byte[] bytes = new byte[2];
      is.mark(2);
      boolean empty = (is.read(bytes) == 0);
      is.reset();
      return empty | (bytes[0] == (byte) GZIPInputStream.GZIP_MAGIC && bytes[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    } catch (Exception e) {
      return false;
    }
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

  protected static class ExcludeFromHash {

  }
}
