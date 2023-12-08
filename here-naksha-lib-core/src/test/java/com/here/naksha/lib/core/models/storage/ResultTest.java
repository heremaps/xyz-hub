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
package com.here.naksha.lib.core.models.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import org.junit.jupiter.api.Test;

public class ResultTest {

  @Test
  void shouldBeAbleToGetForwardCursorBack() throws NoCursor {
    // given
    SuccessResult result = new MockResult<>(new LimitedForwardCursor<>(XyzFeatureCodecFactory.get(), 10));

    // expect
    ForwardCursor<XyzFeature, XyzFeatureCodec> forwardCursor = result.getXyzFeatureCursor();
    assertTrue(forwardCursor.next());

    SeekableCursor<XyzFeature, XyzFeatureCodec> seekableCursor = result.getXyzSeekableCursor();
    assertTrue(seekableCursor.next());

    SeekableCursor<XyzFeature, XyzFeatureCodec> seekableCursor2 = result.getXyzSeekableCursor();
    assertTrue(seekableCursor2.next());
    assertSame(seekableCursor, seekableCursor2);

    ForwardCursor<XyzFeature, XyzFeatureCodec> forwardCursorAgain = result.getXyzFeatureCursor();
    assertEquals(9, forwardCursorAgain.position);
    assertSame(forwardCursor, forwardCursorAgain);
  }
}
