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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.admin.AdminMessage;
import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.rest.admin.messages.TestMessage;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.restassured.response.ResponseBodyExtractionOptions;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.awaitility.Durations;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unused")
public class AdminMessagesApiIT extends RestAssuredTest {

  private static ExecutorService threadPool;

  @BeforeClass
  public static void setup() {
    //Mock necessary configuration values
    Service.configuration = new Config();
    Service.configuration.REMOTE_FUNCTION_REQUEST_TIMEOUT = 26;
    Service.configuration.INSTANCE_COUNT = 1;

    threadPool = new ForkJoinPool(10);
  }

  @AfterClass
  public static void tearDown() {
    threadPool.shutdownNow();
  }

  private static void sendMessageToService(AdminMessage message) {
    sendMessageToService(message, given(), AuthProfile.ACCESS_ADMIN_MESSAGING, NO_CONTENT);
  }

  private static void sendMessageToService(AdminMessage message, AuthProfile profile) {
    sendMessageToService(message, given(), profile, NO_CONTENT);
  }

  private static void sendMessageToService(AdminMessage message, RequestSpecification spec) {
    sendMessageToService(message, spec, AuthProfile.ACCESS_ADMIN_MESSAGING, NO_CONTENT);
  }

  private static void sendMessageToService(AdminMessage message, RequestSpecification spec, AuthProfile profile,
      HttpResponseStatus expectedStatus) {
    spec
        .accept(APPLICATION_JSON)
        //.headers(getAuthHeaders(profile))
        .body(Json.encode(message))
        .when()
        .post("/admin/messages?access_token=" + profile.jwt_string)
        .then()
        .statusCode(expectedStatus.code());
  }

  private static ResponseBodyExtractionOptions loadAndVerifyResponse(String tmpFile, String content) {
    return given()
        .accept(APPLICATION_JSON)
        .when()
        .get("/static/" + tmpFile)
        .then()
        .statusCode(OK.code())
        .body("content", equalTo(content))
        .extract()
        .body();
  }

  @Test
  public void testSendBroadcastMessage() {
    long now = System.currentTimeMillis();
    final String tmpFile = "adminMsgTemporaryFile" + now + ".json";
    final String content = "test content" + now;

    sendMessageToService(new TestMessage(tmpFile, content).withRelay(true));

    Set<String> receiverNodeIds = new HashSet<>();

    //In case the test runs against a load-balancer we must make sure the message was actually broad-casted to all nodes.
    await()
        .atMost(5, TimeUnit.SECONDS)
        .pollExecutorService(threadPool)
        .pollInterval(Durations.ONE_HUNDRED_MILLISECONDS)
        .until(() -> {
          ResponseBodyExtractionOptions response = loadAndVerifyResponse(tmpFile, content);

          receiverNodeIds.add(response.path("receiver.id"));
          int nodeCount = response.path("nodeCount");

          //Try until we reached all different nodes
          return receiverNodeIds.size() >= nodeCount;
        });
  }

  @Test
  public void testSendBroadcastMessageNoAuth() {
    long now = System.currentTimeMillis();
    final String tmpFile = "adminMsgTemporaryFile" + now + ".json";
    final String content = "test content" + now;

    sendMessageToService(new TestMessage(tmpFile, content).withRelay(true), given(), AuthProfile.NO_ACCESS, FORBIDDEN);
  }

  private void testSendTargetedMessageWithSpec(boolean useNodeEndpoint) {
    //First send a brod-cast message to all nodes to be able to get information about one of the nodes
    long now = System.currentTimeMillis();
    final String bcTmpFile = "adminMsgTemporaryFile" + now + ".json";
    final String bcContent = "test content" + now;

    sendMessageToService(new TestMessage(bcTmpFile, bcContent).withRelay(true));
    ResponseBodyExtractionOptions bcResponse = loadAndVerifyResponse(bcTmpFile, bcContent);
    //String nodeId = response.path("receiver.id");
    //String nodeIp = response.path("receiver.ip");
    Node node = JsonObject.mapFrom(bcResponse.path("receiver")).mapTo(Node.class);

    //Send a message dedicated to the node
    now = System.currentTimeMillis();
    final String tmpFile = "adminMsgTemporaryFile" + now + ".json";
    final String content = "test content" + now;

    RequestSpecification spec = useNodeEndpoint ? given()
        .baseUri("http://" + node.ip + "/hub")
        .port(node.port) : given();
    TestMessage msg = new TestMessage(tmpFile, content);
    if (!useNodeEndpoint) {
      msg.withRelay(true);
    }
    sendMessageToService(msg.withDestination(node), spec);

    //In case the test runs against a load-balancer we must try to do the request until we hit the desired node.
    await()
        .atMost(5, TimeUnit.SECONDS)
        .pollExecutorService(threadPool)
        .pollInterval(Durations.ONE_HUNDRED_MILLISECONDS)
        .until(() -> {
          ValidatableResponse r = spec
              .accept(APPLICATION_JSON)
              .when()
              .get("/static/" + tmpFile)
              .then();

          if (r.extract().statusCode() == OK.code()) {
            r.body("content", equalTo(content));
            return true;
          }

          return false;
        });
  }

  //@Test
  public void testSendTargetedMessageToNodeEndpoint() {
    testSendTargetedMessageWithSpec(true);
  }

  //@Test
  public void testSendTargetedMessageThroughPublicEndpoint() {
    testSendTargetedMessageWithSpec(false);
  }
}
