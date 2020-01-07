/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.rest.admin.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.admin.Node;
import io.vertx.ext.web.handler.StaticHandler;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A message solely for testing purposes. The sender can provide some content which the receiver will write to some local file in the
 * webroot folder along with some other data about the received message. As this is a {@link RelayedMessage} it can also be sent from
 * "outside" to a node through a load-balancer. The receiving node will care about relaying it to the final destination node where it
 * actually get's handled.
 */
public class TestMessage extends RelayedMessage {

  private static final Logger logger = LogManager.getLogger();

  private static final ObjectMapper mapper = new ObjectMapper();
  public String temporaryFileName;
  public String content;

  public TestMessage(@JsonProperty("temporaryFileName") String temporaryFileName, @JsonProperty("content") String content) {
    this.temporaryFileName = temporaryFileName;
    this.content = content;
  }

  private String generateFileContent() throws JsonProcessingException {
    Map<String, Object> fileContent = new HashMap<>();
    fileContent.put("content", content);
    fileContent.put("receivedAt", System.currentTimeMillis());
    fileContent.put("receiver", Node.OWN_INSTANCE);
    fileContent.put("nodeCount", Service.configuration.INSTANCE_COUNT);
    fileContent.put("receiverRelayed", relay);
    if (relay) {
      fileContent.put("relayedTo", destination);
    }
    return mapper.writeValueAsString(fileContent);
  }

  @Override
  protected void handleAtDestination() {
    try {
      File f = new File(getWebrootFolder() + File.separator + temporaryFileName);
      BufferedWriter writer = new BufferedWriter(new FileWriter(f));
      writer.write(generateFileContent());
      writer.close();
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
          }
          f.delete();
        }
      }).start();
    } catch (IOException | URISyntaxException e) {
      logger.error("Error handling TestMessage", e);
    }
  }

  @JsonIgnore
  private String getWebrootFolder() throws URISyntaxException {
    return Service.configuration.FS_WEB_ROOT != null ? Service.configuration.FS_WEB_ROOT
        : new File(TestMessage.class.getResource("/build.properties").toURI())
            .getParent() + File.separator + StaticHandler.DEFAULT_WEB_ROOT;
  }
}
