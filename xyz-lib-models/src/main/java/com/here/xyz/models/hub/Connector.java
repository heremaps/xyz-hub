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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.EventHandler;
import com.here.xyz.View;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.implementation.Feature;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A connector is a configuration, which binds an event handler to specific parameters required by the handler for all spaces. This can be
 * credentials, target host and more. Therefore, a connector just configures a specific event handler with parameters, so that it can be
 * used by multiple spaces.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Connector")
public final class Connector extends Feature {

  public static final String NUMBER = "number";

  @JsonCreator
  public Connector(@JsonProperty(Feature.ID) @NotNull String id, @JsonProperty(NUMBER) long number) {
    super(id);
    this.number = number;
    this.params = new HashMap<>();
    this.packages = new ArrayList<>();
    this.active = true;
  }

  public long getNumber() {
    return number;
  }

  public void setNumber(long number) {
    this.number = number;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public String getEventHandler() {
    return eventHandler;
  }

  public void setEventHandler(String eventHandler) {
    this.eventHandler = eventHandler;
  }

  public @NotNull Map<@NotNull String, @Nullable Object> getParams() {
    return params;
  }

  public void setParams(@NotNull Map<@NotNull String, @Nullable Object> params) {
    this.params = params;
  }

  public List<String> getContactEmails() {
    return contactEmails;
  }

  public void setContactEmails(List<String> contactEmails) {
    this.contactEmails = contactEmails;
  }

  public ConnectorForward getForward() {
    return forward;
  }

  public void setForward(ConnectorForward forward) {
    this.forward = forward;
  }

  public void setPackages(@NotNull List<@NotNull String> packages) {
    this.packages = packages;
  }

  public @NotNull List<@NotNull String> getPackages() {
    return packages;
  }

  /**
   * The connector number.
   */
  @JsonProperty(NUMBER)
  @JsonView(All.class)
  private long number;

  /**
   * Whether this connector is active. If set to false, the handler will not be added into the event pipelines of spaces. So all spaces
   * using this connector will bypass this connector. If the connector configures the storage, all requests to spaces using the connector as
   * storage will fail.
   */
  @JsonProperty
  @JsonView(All.class)
  private boolean active;

  /**
   * The ID of the event handler to instantiate. This is resolved using {@link EventHandler#getClass(String)}, usage like: <pre>{@code
   * final EventHandler<?> handler = EventHandler.create(connector.eventHandler, connector.params);
   * pipeline.add(handler);
   * }</pre>
   */
  @JsonProperty
  @JsonView(All.class)
  private String eventHandler;

  /**
   * Connector (event handler) parameters to be provided to the event handler constructor.
   */
  @JsonProperty
  @JsonView(View.Protected.class)
  private @NotNull Map<@NotNull String, @NotNull Object> params;

  /**
   * A list of email addresses of responsible owners for this connector. These email addresses will be used to send potential health
   * warnings and other notifications.
   */
  @JsonProperty
  @JsonView(All.class)
  private @Nullable List<@NotNull String> contactEmails;

  /**
   * An object containing the list of different HTTP headers, cookies and query parameters, and their names, that should be forwarded from
   * Hub to the connector; if anything should be forwarded.
   */
  @JsonProperty
  @JsonView(All.class)
  private @Nullable ConnectorForward forward;

  /**
   * List of packages that this connector belongs to.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  private @NotNull List<@NotNull String> packages;
}