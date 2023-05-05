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

package com.here.xyz.responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.flatbuffers.FlatBufferBuilder;
import com.here.xyz.Payload;
import com.here.xyz.bin.ConnectorPayload;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A wrapper class which is based on {@link XyzResponse} for binary responses from connectors. Internally it uses an actual binary
 * representation for the payload.
 * <p>
 * An instance of {@link ConnectorPayload} will be used internally to convert it to binary form. For all other protocol versions the payload
 * will be encoded as JSON.
 *
 * @see Payload#VERSION
 */
public class BinaryResponse extends XyzResponse {

  @JsonCreator
  public BinaryResponse(@JsonProperty byte @NotNull [] bytes, @JsonProperty @NotNull String mimeType) {
    this.bytes = bytes;
    this.mimeType = mimeType;
    setCalculatedEtag(XyzResponse.calculateEtagFor(bytes));
  }

  public static final String BINARY_SUPPORT_VERSION = "0.6.0";

  @JsonProperty
  private final @NotNull String mimeType;
  @JsonProperty
  private final byte @NotNull [] bytes;

  @JsonIgnore
  private boolean etagNeedsRecalculation;
  @JsonIgnore
  private @Nullable String calculatedEtag;

  public @NotNull String getMimeType() {
    return mimeType;
  }

  public byte @NotNull [] getBytes() {
    return bytes;
  }

  @Override
  public @Nullable String getEtag() {
    if (super.getEtag() != null) {
      return super.getEtag();
    }
    if (etagNeedsRecalculation) {
      setCalculatedEtag(XyzResponse.calculateEtagFor(getBytes()));
    }
    return calculatedEtag;
  }

  @JsonIgnore
  private void setCalculatedEtag(@Nullable String etag) {
    calculatedEtag = etag;
    etagNeedsRecalculation = false;
  }

  @Nonnull
  @Override
  public byte @NotNull [] toByteArray() {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int payload = ConnectorPayload.createConnectorPayload(builder, builder.createString(getMimeType()), builder.createString(getEtag()),
        builder.createByteVector(getBytes()));
    builder.finish(payload);
    return buffer2ByteArray(builder.dataBuffer());
  }

  /**
   * Deserializes a binary response from the connector.
   *
   * @param byteArray The bytes coming in from a connector
   * @return The binary response.
   */
  public static @NotNull BinaryResponse fromByteArray(byte @NotNull [] byteArray) {
    final ConnectorPayload payload = ConnectorPayload.getRootAsConnectorPayload(ByteBuffer.wrap(byteArray));
    final ByteBuffer byteBuffer = payload.bytesAsByteBuffer();
    final byte[] bytes = buffer2ByteArray(byteBuffer);
    final String mimeType = payload.mimeType();
    assert mimeType != null;
    final BinaryResponse binaryResponse = new BinaryResponse(bytes, mimeType);
    binaryResponse.setEtag(payload.etag());
    return binaryResponse;
  }

  private static byte @NotNull [] buffer2ByteArray(@NotNull ByteBuffer buffer) {
    byte[] byteArray = new byte[buffer.remaining()];
    buffer.get(byteArray);
    return byteArray;
  }
}
