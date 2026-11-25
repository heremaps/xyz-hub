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
import static org.junit.Assert.assertTrue;

import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.jwt.JWTPayload;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.util.service.errors.DetailedHttpException;
import com.here.xyz.util.service.errors.ErrorManager;
import io.vertx.core.Future;
import org.junit.BeforeClass;
import org.junit.Test;

public class TagApiTest {

  @BeforeClass
  public static void loadErrors() {
    try {
      ErrorManager.loadErrors("hub-errors.json");
    }
    catch (RuntimeException e) {
      //Another test class in the same JVM loaded them already
      if (!(e.getCause() instanceof IllegalStateException) || !e.getCause().getMessage().contains("already registered"))
        throw e;
    }
  }
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

  @Test
  public void anonymousTokenIsNotUsed() {
    //The token of the DUMMY authorization, as used by a Hub embedded behind a gateway
    assertNull(TagApi.resolveAuthor(null, jwt("ANONYMOUS", null)));
    JWTPayload anonymous = jwt("account", "client");
    anonymous.anonymous = true;
    assertNull(TagApi.resolveAuthor(null, anonymous));
    assertEquals("author", TagApi.resolveAuthor("author", jwt("ANONYMOUS", null)));
  }

  @Test
  public void tagCanNotPointToRangeAllVersionsOrTag() {
    for (String ref : new String[]{"1..3", "1..HEAD", "*", "someTag"}) {
      //Fails before the space is read, so no storage is needed
      Future<Ref> result = TagApi.resolveVersionRef(null, "space", new Ref(ref));
      assertTrue(ref, result.failed());
      DetailedHttpException e = (DetailedHttpException) result.cause();
      assertEquals(ref, "E318404", e.errorDefinition.getCode());
      assertEquals(400, e.status.code());
    }
  }

  @Test
  public void negativeVersionIsRejected() {
    for (String body : new String[]{"{\"version\":-10}", "{\"version\":-1}", "{\"versionRef\":\"-10\"}"}) {
      Future<Tag> result = TagApi.deserializeTag(body);
      assertTrue(body, result.failed());
      DetailedHttpException e = (DetailedHttpException) result.cause();
      assertEquals(body, "E318404", e.errorDefinition.getCode());
      assertEquals(body, 400, e.status.code());
      assertEquals(body, "The version of a tag must not be lower than 0.", e.placeholders.get("cause"));
    }
  }

  @Test
  public void rangeInBodyIsStillRejectedAsRange() {
    Future<Tag> result = TagApi.deserializeTag("{\"versionRef\":\"1..3\"}");
    assertTrue(result.failed());
    DetailedHttpException e = (DetailedHttpException) result.cause();
    assertEquals("E318404", e.errorDefinition.getCode());
    assertEquals("1..3", e.placeholders.get("versionRef"));
    assertTrue(e.placeholders.get("cause").startsWith("A tag must point to a single version or HEAD"));
  }

  @Test
  public void validVersionsAreAccepted() {
    assertTrue(TagApi.deserializeTag("{\"version\":0}").result().getVersionRef().isSingleVersion());
    assertEquals(5, TagApi.deserializeTag("{\"version\":5}").result().getVersionRef().getVersion());
    assertEquals(5, TagApi.deserializeTag("{\"versionRef\":\"5\"}").result().getVersionRef().getVersion());
    //No version at all means HEAD
    assertTrue(TagApi.deserializeTag("{\"id\":\"t\"}").result().getVersionRef().isHead());
  }

  @Test
  public void versionIsNotVerified() {
    //Resolved without reading the space, also if the version does not exist (yet)
    Future<Ref> result = TagApi.resolveVersionRef(null, "space", new Ref(999));
    assertTrue(result.succeeded());
    assertEquals(999, result.result().getVersion());
  }

  @Test
  public void updateKeepsAuthorAndCreationTime() {
    Tag stored = new Tag().withAuthor("creator").withCreatedAt(1000);
    assertEquals("creator", TagApi.keptAuthor(stored, "updater"));
    assertEquals(1000, TagApi.keptCreatedAt(stored));
  }

  @Test
  public void updateBackfillsMissingAuthorAndCreationTime() {
    //How DynamoTagConfigClient reports a tag stored without author and creation time
    Tag stored = new Tag().withAuthor("system").withCreatedAt(-1);
    assertEquals("updater", TagApi.keptAuthor(stored, "updater"));
    assertTrue(TagApi.keptCreatedAt(stored) > 0);
    assertEquals("", TagApi.keptAuthor(new Tag().withAuthor(""), null));
    assertEquals("updater", TagApi.keptAuthor(new Tag().withAuthor(""), "updater"));
  }
}
