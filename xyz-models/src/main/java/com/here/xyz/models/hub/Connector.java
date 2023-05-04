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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.EventHandler;
import com.here.xyz.View;
import com.here.xyz.View.All;
import com.here.xyz.models.geojson.implementation.AbstractFeature;
import com.here.xyz.models.geojson.implementation.Properties;
import java.util.ArrayList;
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
public class Connector extends AbstractFeature<Properties, Connector> {

  public Connector() {
    this.properties = new Properties();
  }

  /**
   * The connector number.
   */
  @JsonProperty
  @JsonView(All.class)
  public long number;

  /**
   * Whether this connector is active. If set to false, the handler will not be added into the event pipelines of spaces. So all spaces
   * using this connector will bypass this connector. If the connector configures the storage, all requests to spaces using the connector as
   * storage will fail.
   */
  @JsonProperty
  @JsonView(All.class)
  public boolean active = true;

  /**
   * The ID of the event handler to instantiate. This is resolved using {@link EventHandler#getClass(String)}, usage like: <pre>{@code
   * final EventHandler<?> handler = EventHandler.create(connector.eventHandler, connector.params);
   * pipeline.add(handler);
   * }</pre>
   */
  @JsonProperty
  @JsonView(All.class)
  public String eventHandler;

  /**
   * Connector (event handler) parameters to be provided to the event handler constructor.
   */
  @JsonProperty
  @JsonView(View.Protected.class)
  public Map<@NotNull String, @NotNull Object> params;

  /**
   * A list of email addresses of responsible owners for this connector. These email addresses will be used to send potential health
   * warnings and other notifications.
   */
  @JsonProperty
  @JsonView(All.class)
  public @Nullable List<@NotNull String> contactEmails;

  /**
   * An object containing the list of different HTTP headers, cookies and query parameters, and their names, that should be forwarded from
   * Hub to the connector; if anything should be forwarded.
   */
  @JsonProperty
  @JsonView(All.class)
  public @Nullable ConnectorForward forward;

  /**
   * List of packages that this connector belongs to.
   */
  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_EMPTY)
  public List<@NotNull String> packages;

  public @NotNull List<@NotNull String> getPackages() {
    List<@NotNull String> packages = this.packages;
    if (packages == null) {
      this.packages = packages = new ArrayList<>();
    }
    return packages;
  }

  @Override
  protected @NotNull Properties newProperties() {
    return new Properties();
  }
}