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
package com.here.naksha.lib.core.models.features;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.extension.ExtensionHandler;
import com.here.naksha.lib.core.models.IPlugin;
import com.here.naksha.lib.core.models.PluginCache;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.lang.reflect.InvocationTargetException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A configured event handler. The code selected via {@link #className} will use the given properties as configuration parameters.
 */
@AvailableSince(NakshaVersion.v2_0_3)
@JsonTypeName(value = "Connector")
public class Connector extends XyzFeature implements IPlugin<IEventHandler> {

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String CLASS_NAME = "className";

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String EXTENSION = "extension";

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String ACTIVE = "active";

  /**
   * Create a new connector.
   *
   * @param id    the identifier of the event handler.
   * @param cla$$ the class, that implements this event handler.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  public Connector(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(CLASS_NAME) @NotNull Class<? extends IEventHandler> cla$$) {
    super(id);
    this.className = cla$$.getName();
  }

  /**
   * Create a new connector.
   *
   * @param id        the identifier of the event handler.
   * @param className the full qualified name of the class, that implements this handler.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonCreator
  public Connector(@JsonProperty(ID) @NotNull String id, @JsonProperty(CLASS_NAME) @NotNull String className) {
    super(id);
    this.className = className;
  }

  /**
   * The classname to load, the class must implement the {@link IEventHandler} interface, and the constructor must accept exactly one
   * parameter of the type {@link Connector}. It may throw any exception.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonProperty(CLASS_NAME)
  private @NotNull String className;

  /**
   * If this connector is an extension, then this holds the extension identification number; a 16-bit unsigned integer.
   */
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
  private boolean active;

  @Override
  public @NotNull IEventHandler newInstance() throws Exception {
    try {
      return PluginCache.newInstance(className, IEventHandler.class, this);
    } catch (InvocationTargetException ite) {
      if (ite.getCause() instanceof Exception e) {
        throw e;
      }
      throw ite;
    }
  }

  /**
   * Internally used by Naksha-Hub as replacement for {@link #newInstance()}, because this will return an internal proxy handler, when this
   * is a connector being part of an extension.
   *
   * @return the event-handler.
   * @throws Exception if any error occurred.
   */
  @NotNull
  IEventHandler newInstanceOrExtensionHandler() throws Exception {
    try {
      if (extension > 0) {
        return new ExtensionHandler(this);
      }
      return PluginCache.newInstance(className, IEventHandler.class, this);
    } catch (InvocationTargetException ite) {
      if (ite.getCause() instanceof Exception e) {
        throw e;
      }
      throw ite;
    }
  }

  public @NotNull String getClassName() {
    return className;
  }

  public void setClassName(@NotNull String className) {
    this.className = className;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public int getExtension() {
    return extension;
  }

  public void setExtension(int extension) {
    this.extension = extension;
  }
}
