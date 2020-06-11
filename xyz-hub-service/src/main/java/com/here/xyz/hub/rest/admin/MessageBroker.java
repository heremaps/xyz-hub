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

package com.here.xyz.hub.rest.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.hub.rest.admin.messages.brokers.RedisMessageBroker;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The MessageBroker provides the infrastructural implementation of how to send
 * & receive {@link AdminMessage}s.
 * 
 * NOTE: The {@link MessageBroker#getInstance()} method decides which
 * implementation to return.
 * 
 * The default {@link MessageBroker} implementation is the
 * {@link SnsMessageBroker}.
 * 
 * To set the {@link MessageBroker} you can set the environment variable
 * ADMIN_MESSAGE_BROKER e.g.
 * "ADMIN_MESSAGE_BROKER={@link StaticWebMessageBroker}" or
 * "ADMIN_MESSAGE_BROKER={@link ServiceDiscoveryWebMessageBroker}".
 * 
 */
public interface MessageBroker {

  Logger logger = LogManager.getLogger();
  ThreadLocal<ObjectMapper> mapper = ThreadLocal.withInitial(ObjectMapper::new);

  void sendRawMessage(String jsonMessage);

  default void sendMessage(AdminMessage message) {
    if (!Node.OWN_INSTANCE.equals(message.destination)) {
      String jsonMessage = null;
      try {
        jsonMessage = mapper.get().writeValueAsString(message);
        sendRawMessage(jsonMessage);
      }
      catch (JsonProcessingException e) {
        logger.error("Error while serializing AdminMessage of type {} prior to send it.", message.getClass().getSimpleName());
      }
      catch (Exception e) {
        logger.error("Error while sending AdminMessage: {}", jsonMessage);
      }
    }
    //Receive it (also) locally (if applicable)
    /*
    NOTE: Local messages will always be received directly and only once. This is also true for a broadcast message
    with the #broadcastIncludeLocalNode flag being active.
     */
    receiveMessage(message);
  }

  default void receiveRawMessage(byte[] rawJsonMessage) {
    if (rawJsonMessage == null) {
      logger.error("No bytes given for receiving the message.", new NullPointerException());
      return;
    }
    receiveRawMessage(new String(rawJsonMessage));
  }

  default void receiveRawMessage(String jsonMessage) {
    receiveMessage(deserializeMessage(jsonMessage));
  }

  default AdminMessage deserializeMessage(String jsonMessage) {
    AdminMessage message = null;
    try {
      message = mapper.get().readValue(jsonMessage, AdminMessage.class);
    }
    catch (IOException e) {
      logger.error("Error while de-serializing AdminMessage {} : {}", jsonMessage, e);
    }
    catch (Exception e) {
      logger.error("Error while receiving AdminMessage {} : {}", jsonMessage, e);
    }
    return message;
  }

  default void receiveMessage(AdminMessage message) {
    if (message == null)
      return;
    if (message.source == null)
      throw new NullPointerException("The source node of the AdminMessage must be defined.");

    if (message.destination == null && (!Node.OWN_INSTANCE.equals(message.source) || message.broadcastIncludeLocalNode)
        || Node.OWN_INSTANCE.equals(message.destination)) {
      try {
        message.handle();
      }
      catch (RuntimeException e) {
        logger.error("Error while trying to handle AdminMessage {} : {}", message, e);
      }
    }
  }

  static MessageBroker getInstance() {
    //Return an instance of the default implementation
    return RedisMessageBroker.getInstance();
  }
}
