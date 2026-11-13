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

package com.here.xyz.hub.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.here.xyz.models.hub.jwt.JWTPayload;
import org.junit.Test;

public class TagApiTest {
  private static JWTPayload jwt(String aid, String cid) {
    JWTPayload jwt = new JWTPayload();
    jwt.aid = aid;
    jwt.cid = cid;
    return jwt;
  }

  @Test
  public void authorOfRequestIsUsed() {
    assertEquals("author", TagApi.resolveAuthor("author", jwt("account", "client")));
  }

  @Test
  public void accountOfTokenIsUsedWithoutRequestAuthor() {
    assertEquals("account", TagApi.resolveAuthor(null, jwt("account", "client")));
    assertEquals("account", TagApi.resolveAuthor("  ", jwt("account", "client")));
  }

  @Test
  public void clientOfTokenIsUsedWithoutAccount() {
    assertEquals("client", TagApi.resolveAuthor(null, jwt(null, "client")));
  }

  @Test
  public void noAuthorAtAll() {
    assertNull(TagApi.resolveAuthor(null, jwt(null, null)));
    assertNull(TagApi.resolveAuthor(null, null));
  }
}
