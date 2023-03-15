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

package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.events.Event;
import com.here.xyz.models.hub.Space.ConnectorRef;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configuration, which binds a processor to specific parameters required by the processor for all spaces. This can be credentials, target
 * host and more. Therefore, a connector just configures a specific processor so that it can be used by multiple spaces as storage.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Connector {

  protected static final Logger logger = LoggerFactory.getLogger(Connector.class);

  /**
   * The unique identifier of this connector configuration.
   */
  @JsonProperty
  public String id;

  /**
   * Whether this connector is active. If set to false, no events will are send to the processor, so all spaces using this connector will
   * respond with an error.
   */
  @JsonProperty
  public boolean active = true;

  /**
   * The processor to use. This must be one value from {@link Processor}, usage like: <pre>{@code
   * final Processor p = Processor.get(connector.processor);
   * }</pre>
   */
  @JsonProperty
  public String processor;

  /**
   * Connector (processor) parameters to be provided to the processor.
   *
   * <p>The processor will receive these parameters in {@link Event#getConnectorParams()} plus the
   * {@link ConnectorRef#getParams() parameters of the space} in {@link Event#getParams()}.
   */
  @JsonProperty
  public @Nullable Map<@NotNull String, @NotNull Object> params;

  /**
   * A list of email addresses of responsible owners for this connector. These email addresses will be used to send potential health
   * warnings and other notifications.
   */
  public List<String> contactEmails;

  /**
   * An object containing the list of different HTTP headers, cookies and query parameters, and their names, that should be forwarded from
   * Hub to the connector; if anything should be forwarded.
   */
  public ConnectorForward forward;
}
