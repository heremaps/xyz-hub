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

import com.here.xyz.hub.Service;

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

  void sendMessage(AdminMessage message);

  void receiveRawMessage(byte[] rawJsonMessage);

  static MessageBroker getInstance() {
    switch (Service.configuration.ADMIN_MESSAGE_BROKER != null ? Service.configuration.ADMIN_MESSAGE_BROKER
        : "SnsMessageBroker") {
      case "StaticWebMessageBroker":
        return StaticWebMessageBroker.getInstance();
      case "S3WebMessageBroker":
        return S3WebMessageBroker.getInstance();
      case "ServiceDiscoveryWebMessageBroker":
        return ServiceDiscoveryWebMessageBroker.getInstance();
      case "TargetGroupWebMessageBroker":
        return TargetGroupWebMessageBroker.getInstance();
      case "SnsMessageBroker":
      default:
        return SnsMessageBroker.getInstance();
    }
  }
}
