/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.here.xyz.util.service.HttpException;
import org.junit.Test;

public class FeatureQueryApiH3IndexTest {

  @Test
  public void validH3IndexFormats() throws HttpException {
    FeatureQueryApi.validateH3Index(null);
    FeatureQueryApi.validateH3Index("871faeba8ffffff");
    FeatureQueryApi.validateH3Index("0871faeba8ffffff");
    FeatureQueryApi.validateH3Index("871FAEBA8FFFFFF");
  }

  @Test
  public void malformedH3IndexIsABadRequest() {
    for (String h3Index : new String[]{"", "zz", "871faeba8fffff", "871faeba8ffffff12", "871faeba8fffffg", " 871faeba8ffffff"}) {
      HttpException e = assertThrows(h3Index, HttpException.class, () -> FeatureQueryApi.validateH3Index(h3Index));
      assertEquals(h3Index, BAD_REQUEST, e.status);
    }
  }
}
