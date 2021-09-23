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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.flatbuffers.FlatBufferBuilder;
import com.here.xyz.Payload;
import com.here.xyz.bin.ConnectorPayload;
import java.nio.ByteBuffer;

/**
 * A wrapper class which is based on {@link XyzResponse} for binary responses from connectors.
 * Internally it uses an actual binary representation for the payload.
 *
 * An instance of {@link ConnectorPayload} will be used internally to convert it to binary form.
 * For all other protocol versions the payload will be encoded as JSON.
 *
 * @see Payload#VERSION
 */
public class BinaryResponse extends XyzResponse<BinaryResponse> {

  public static final String BINARY_SUPPORT_VERSION = "0.6.0";

  private String mimeType;
  private byte[] bytes;

  @JsonIgnore
  private boolean etagNeedsRecalculation;
  @JsonIgnore
  private String calculatedEtag;

  public String getMimeType() {
    return mimeType;
  }

  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }

  public BinaryResponse withMimeType(String mimeType) {
    setMimeType(mimeType);
    return this;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public void setBytes(byte[] bytes) {
    etagNeedsRecalculation = true;
    this.bytes = bytes;
  }

  public BinaryResponse withBytes(byte[] bytes) {
    setBytes(bytes);
    return this;
  }

  @Override
  public String getEtag() {
    if (super.getEtag() != null)
      return super.getEtag();
    if (etagNeedsRecalculation)
      setCalculatedEtag(getBytes() == null ? null : XyzResponse.calculateEtagFor(getBytes()));
    return calculatedEtag;
  }

  @JsonIgnore
  private void setCalculatedEtag(String etag) {
    calculatedEtag = etag;
    etagNeedsRecalculation = false;
  }

  @Override
  public byte[] toByteArray() {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int payload = ConnectorPayload.createConnectorPayload(builder, builder.createString(getMimeType()), builder.createString(getEtag()), builder.createByteVector(getBytes()));
    builder.finish(payload);
    return buffer2ByteArray(builder.dataBuffer());
  }

  /**
   * Deserializes a binary response from the connector.
   * @param byteArray The bytes coming in from a connector
   * @return
   */
  public static BinaryResponse fromByteArray(byte[] byteArray) {
    ConnectorPayload payload = ConnectorPayload.getRootAsConnectorPayload(ByteBuffer.wrap(byteArray));
    return new BinaryResponse()
        .withMimeType(payload.mimeType())
        .withBytes(buffer2ByteArray(payload.bytesAsByteBuffer()))
        .withEtag(payload.etag());
  }

  private static final byte[] buffer2ByteArray(ByteBuffer buffer) {
    byte[] byteArray = new byte[buffer.remaining()];
    buffer.get(byteArray);
    return byteArray;
  }
}
