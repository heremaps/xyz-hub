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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.models.payload.responses.BinaryResponse;
import org.junit.jupiter.api.Test;

public class TestBinaryResponse {

    private static final String MIME_TYPE = "text/plain";
    private static final String SAMPLE_TEXT = "Some sample Text.";
    private static final String SAMPLE_ETAG = "someEtag";
    private static final String CALCULATED_ETAG = "\"8647bbe6947d2ecddd12924664d1574a\"";
    private static final byte[] SAMPLE_BYTES = {
        16, 0, 0, 0, 0, 0, 10, 0, 16, 0, 4, 0, 8, 0, 12, 0, 10, 0, 0, 0, 76, 0, 0, 0, 32, 0, 0, 0, 4, 0, 0, 0, 17, 0, 0,
        0, 83, 111, 109, 101, 32, 115, 97, 109, 112, 108, 101, 32, 84, 101, 120, 116, 46, 0, 0, 0, 34, 0, 0, 0, 34, 56,
        54, 52, 55, 98, 98, 101, 54, 57, 52, 55, 100, 50, 101, 99, 100, 100, 100, 49, 50, 57, 50, 52, 54, 54, 52, 100,
        49, 53, 55, 52, 97, 34, 0, 0, 10, 0, 0, 0, 116, 101, 120, 116, 47, 112, 108, 97, 105, 110, 0, 0
    };

    @Test
    public void testSerialize() {
        final BinaryResponse br = new BinaryResponse(SAMPLE_TEXT.getBytes(), MIME_TYPE);
        assertEquals(CALCULATED_ETAG, br.getEtag());
        assertArrayEquals(SAMPLE_BYTES, br.toByteArray());
    }

    @Test
    public void testDeserialize() {
        BinaryResponse br = BinaryResponse.fromByteArray(SAMPLE_BYTES);
        assertEquals(CALCULATED_ETAG, br.getEtag());
        assertEquals(MIME_TYPE, br.getMimeType());
        assertEquals(SAMPLE_TEXT, new String(br.getBytes()));
    }

    @Test
    public void testOverrideEtag() {
        final BinaryResponse br = new BinaryResponse(SAMPLE_TEXT.getBytes(), MIME_TYPE);
        br.setEtag(SAMPLE_ETAG);
        assertEquals(SAMPLE_ETAG, br.getEtag());
    }
}
