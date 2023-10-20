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
package com.here.naksha.lib.core.models.naksha;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.PluginCache;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A configured event handler.
 */
@AvailableSince(NakshaVersion.v2_0_3)
@JsonTypeName(value = "Connector")
public class EventHandler extends Plugin<IEventHandler, EventHandler> {

  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String EXTENSION = "extension";

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String ACTIVE = "active";

  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_6)
  public static final String URL = "url";

  /**
   * Create a new connector.
   *
   * @param id    the identifier of the event handler.
   * @param cla$$ the class, that implements this event handler.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  public EventHandler(
      @JsonProperty(CLASS_NAME) @NotNull Class<? extends IEventHandler> cla$$,
      @JsonProperty(ID) @NotNull String id) {
    super(cla$$.getName(), id);
  }

  /**
   * Create a new connector.
   *
   * @param id        the identifier of the event handler.
   * @param className the full qualified name of the class, that implements this handler.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonCreator
  public EventHandler(@JsonProperty(CLASS_NAME) @NotNull String className, @JsonProperty(ID) @NotNull String id) {
    super(className, id);
  }

  /**
   * If this connector is an extension, then this holds the extension identification number; a 16-bit unsigned integer.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonProperty(EXTENSION)
  @JsonInclude(Include.NON_DEFAULT)
  private int extension;

  /**
   * Whether this connector is active. If set to false, the handler will not be added into the event pipelines of spaces. So all spaces
   * using this connector will bypass this connector. If the connector configures the storage, all requests to spaces using the connector as
   * storage will fail.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonProperty(ACTIVE)
  private boolean active = true;

  public @Nullable String getExtensionId() {
    return extensionId;
  }

  public void setExtensionId(@Nullable String extensionId) {
    this.extensionId = extensionId;
  }

  /**
   * The unique identifier of the extension that hosts the handler, referred by the {@link #getClassName() className}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  private @Nullable String extensionId;

  /**
   * The connectivity details required by this connector to perform feature operations. If connector is attached to a Storage (using
   * storageId), then this is storage credentials.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_6)
  @JsonProperty(URL)
  private String url;

  @AvailableSince(NakshaVersion.v2_0_7)
  @Override
  public @NotNull IEventHandler newInstance(@NotNull INaksha naksha) {
    return PluginCache.newInstance(getClassName(), IEventHandler.class, this);
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  @Deprecated
  public int getExtension() {
    return extension;
  }

  @Deprecated
  public void setExtension(int extension) {
    this.extension = extension;
  }

  @Deprecated
  public @NotNull String getUrl() {
    return url;
  }

  @Deprecated
  public void setUrl(@NotNull String url) {
    this.url = url;
  }
}
