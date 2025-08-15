/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.flatbuffers.FlatBufferBuilder;
import com.here.xyz.XyzSerializable;
import com.here.xyz.bin.BinEvent;
import java.nio.ByteBuffer;

/**
 * This is an event type that is able to carry a binary buffer alongside arbitrary "meta" information from the event type.
 * All event fields will be serialized and stored in a field called "event" inside the binary {@link BinEvent}.
 * @param <T> The extending event type
 */
public abstract class BinaryEvent<T extends BinaryEvent> extends ContextAwareEvent<T> {

  public static final String BINARY_IDENTIFIER = "XYZB";
  @JsonIgnore
  private String mimeType = "application/octet-stream";
  @JsonIgnore
  private byte[] bytes;

  public String getMimeType() {
    return mimeType;
  }

  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }

  public T withMimeType(String mimeType) {
    setMimeType(mimeType);
    return (T) this;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public T withBytes(byte[] bytes) {
    setBytes(bytes);
    return (T) this;
  }

  public static byte[] buffer2ByteArray(ByteBuffer buffer) {
    byte[] byteArray = new byte[buffer.remaining()];
    buffer.get(byteArray);
    return byteArray;
  }

  public static boolean isXyzBinaryPayload(byte[] bytes) {
    if (bytes == null || bytes.length < 8)
      return false;
    return BINARY_IDENTIFIER.equals(new StringBuilder()
        .append((char) bytes[4])
        .append((char) bytes[5])
        .append((char) bytes[6])
        .append((char) bytes[7])
        .toString());
  }

  public static boolean isXyzBinaryPayload(String magicNumber) {
    return BINARY_IDENTIFIER.equals(magicNumber);
  }

  /**
   * Serializes a BinaryEvent to send it to the connector.
   * @return The byte array containing the binary representation of the event
   */
  @Override
  public byte[] toByteArray() {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int payload = BinEvent.createBinEvent(builder, builder.createString(serialize()), builder.createString(getMimeType()), builder.createByteVector(getBytes()));
    builder.finish(payload, BINARY_IDENTIFIER);
    return buffer2ByteArray(builder.dataBuffer());
  }

  /**
   * Deserializes a BinaryEvent within the connector.
   * @param byteArray The bytes coming in the service
   * @return
   */
  public static BinaryEvent fromByteArray(byte[] byteArray) throws JsonProcessingException {
    BinEvent binaryEvent = BinEvent.getRootAsBinEvent(ByteBuffer.wrap(byteArray));
    return XyzSerializable.<BinaryEvent>deserialize(binaryEvent.event())
        .withMimeType(binaryEvent.mimeType())
        .withBytes(buffer2ByteArray(binaryEvent.bytesAsByteBuffer()));
  }
}
