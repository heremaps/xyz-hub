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

package com.here.xyz.bin;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class TestConnectorPayload {

    private static final byte[] SAMPLE_BYTES = {
        16, 0, 0, 0, 0, 0, 10, 0, 16, 0, 4, 0, 8, 0, 12, 0, 10, 0, 0, 0, 52, 0, 0, 0, 32, 0, 0, 0, 4, 0, 0, 0, 18, 0, 0,
        0, 115, 111, 109, 101, 32, 115, 97, 109, 112, 108, 101, 32, 115, 116, 114, 105, 110, 103, 0, 0, 8, 0, 0, 0, 116,
        101, 115, 116, 69, 116, 97, 103, 0, 0, 0, 0, 16, 0, 0, 0, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110,
        47, 106, 115, 111, 110, 0, 0, 0, 0
    };

    @Test
    public void testReadBytes() {
        ConnectorPayload payload = ConnectorPayload.getRootAsConnectorPayload(ByteBuffer.wrap(SAMPLE_BYTES));
        assertEquals("application/json", payload.mimeType());
        assertEquals("testEtag", payload.etag());
        assertEquals(
                "some sample string",
                StandardCharsets.UTF_8.decode(payload.bytesAsByteBuffer()).toString());
    }

    @Test
    public void testWriteBytes() {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int payload = ConnectorPayload.createConnectorPayload(
                builder,
                builder.createString("application/json"),
                builder.createString("testEtag"),
                builder.createByteVector("some sample string".getBytes()));
        builder.finish(payload);

        byte[] byteArray = new byte[builder.dataBuffer().remaining()];
        builder.dataBuffer().get(byteArray);

        assertArrayEquals(SAMPLE_BYTES, byteArray);
    }
}
