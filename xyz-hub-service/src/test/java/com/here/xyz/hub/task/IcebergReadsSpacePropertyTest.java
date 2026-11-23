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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.task.IcebergReadMode.SPACE_PROPERTY;
import static com.here.xyz.models.hub.FeatureModificationList.IfExists.PATCH;
import static com.here.xyz.models.hub.FeatureModificationList.IfNotExists.ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Public;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.SpaceAuthorization;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.SpaceTask.ConditionalOperation;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Internal;
import com.here.xyz.models.hub.Space.WithConnectors;
import com.here.xyz.models.hub.jwt.JWTPayload;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.json.jackson.DatabindCodec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The space property {@code icebergReads}: internal, validated and only set by Hub admins.
 */
class IcebergReadsSpacePropertyTest {

  private static final String OWNER = "test-owner";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Config previousConfiguration;

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    Service.configuration = new Config();
  }

  @AfterEach
  void tearDown() {
    Service.configuration = previousConfiguration;
  }

  @Test
  void thePropertyIsNotInThePublicView() throws Exception {
    Space space = space("ON");
    assertThat(json(space, Public.class).has(SPACE_PROPERTY)).isFalse();
    assertThat(json(space, WithConnectors.class).has(SPACE_PROPERTY)).isFalse();
    assertThat(json(space, Internal.class).path(SPACE_PROPERTY).asText()).isEqualTo("ON");
    //The stored representation
    assertThat(json(space, Static.class).path(SPACE_PROPERTY).asText()).isEqualTo("ON");
    assertThat(json(space(null), Static.class).has(SPACE_PROPERTY)).isFalse();
  }

  @Test
  void thePropertyIsReadFromTheStoredRepresentation() throws Exception {
    Space space = XyzSerializable.deserialize(XyzSerializable.serialize(space("SHADOW"), Static.class), Space.class);
    assertThat(space.getIcebergReads()).isEqualTo("SHADOW");
  }

  @Test
  void onlyTheModesAreValid() throws Exception {
    for (String value : new String[]{null, "OFF", "ON", "SHADOW"})
      SpaceTaskHandler.validate(update(null, value, new JWTPayload()), failingCallback());
    for (String value : new String[]{"on", "unknown", ""})
      assertThatThrownBy(() -> SpaceTaskHandler.validate(update(null, value, new JWTPayload()), failingCallback()))
          .as(value)
          .isInstanceOfSatisfying(HttpException.class, e -> assertThat(e.status).isEqualTo(BAD_REQUEST))
          .hasMessageContaining(SPACE_PROPERTY);
  }

  @Test
  void anAdminMaySetAndChangeTheProperty() throws Exception {
    assertThat(authorize(update(null, "ON", admin()))).isNull();
    assertThat(authorize(update("ON", "SHADOW", admin()))).isNull();
    assertThat(authorize(update("SHADOW", null, admin()))).isNull();
  }

  @Test
  void aSpaceManagerMayNotSetOrChangeTheProperty() throws Exception {
    for (String[] change : new String[][]{{null, "ON"}, {"OFF", "ON"}, {"ON", null}}) {
      Throwable error = authorize(update(change[0], change[1], manager()));
      assertThat(error).as(change[0] + " -> " + change[1])
          .isInstanceOfSatisfying(HttpException.class, e -> assertThat(e.status).isEqualTo(FORBIDDEN));
    }
  }

  @Test
  void aSpaceManagerMayStillChangeOtherPropertiesOfASpaceWithTheProperty() throws Exception {
    ConditionalOperation task = update("ON", "ON", manager());
    task.modifyOp.entries.get(0).result.setTitle("changed");
    assertThat(authorize(task)).isNull();
  }

  /**
   * The JSON of the space in a view, as the space API writes it
   */
  private static JsonNode json(Space space, Class<?> view) throws Exception {
    return MAPPER.readTree(DatabindCodec.mapper().writerWithView(view).writeValueAsString(space));
  }

  private static Space space(String icebergReads) {
    Space space = new Space();
    space.setId("my-space");
    space.setTitle("title");
    space.setOwner(OWNER);
    space.setStorage(new ConnectorRef().withId("psql"));
    space.setIcebergReads(icebergReads);
    return space;
  }

  /**
   * A PATCH of the space which changes the property from {@code from} to {@code to}
   */
  private static ConditionalOperation update(String from, String to, JWTPayload jwt) {
    Map<String, Object> input = new HashMap<>(Map.of("id", "my-space"));
    input.put(SPACE_PROPERTY, to);
    ModifySpaceOp modifyOp = new ModifySpaceOp(List.of(input), ERROR, PATCH, true, false);
    modifyOp.entries.get(0).head = space(from);
    modifyOp.entries.get(0).result = space(to);
    Map<String, Object> data = new HashMap<>();
    ConditionalOperation task = new ConditionalOperation(IcebergReadTestSupport.routingContext(data, "my-space"),
        ApiResponseType.SPACE, modifyOp, true);
    data.put("jwt", jwt);
    return task;
  }

  /**
   * @return The error of the authorization, or null if it passed
   */
  private static Throwable authorize(ConditionalOperation task) throws Exception {
    CompletableFuture<Throwable> outcome = new CompletableFuture<>();
    SpaceAuthorization.authorizeModifyOp(task, new Callback<>() {
      @Override
      public void exception(Throwable e) {
        outcome.complete(e);
      }

      @Override
      public void call(ConditionalOperation value) {
        outcome.complete(null);
      }
    });
    return outcome.get(5, TimeUnit.SECONDS);
  }

  private static JWTPayload admin() {
    return jwt("manageSpaces", "adminSpaces");
  }

  private static JWTPayload manager() {
    return jwt("manageSpaces");
  }

  private static JWTPayload jwt(String... actions) {
    Map<String, Object> rights = new HashMap<>();
    for (String action : actions)
      rights.put(action, List.of(Map.of("owner", OWNER)));
    return DatabindCodec.mapper().convertValue(Map.of("aid", OWNER, "urm", Map.of("xyz-hub", rights)), JWTPayload.class);
  }

  private static Callback<ConditionalOperation> failingCallback() {
    return new Callback<>() {
      @Override
      public void exception(Throwable e) {
        throw new AssertionError(e);
      }

      @Override
      public void call(ConditionalOperation value) {}
    };
  }
}
