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
package com.here.naksha.lib.core.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The standard properties of the standard feature store in the Naksha-Hub. */
public class Properties extends JsonObject {

  /**
   * The internal management properties of the Naksha-Hub.
   *
   * @since 0.1.0
   */
  public static final String XYZ_NAMESPACE = "@ns:com:here:xyz";

  /**
   * Properties used by the deprecated Activity-Log service, just here to allow downward
   * compatibility.
   *
   * @since 0.6.0
   */
  public static final String XYZ_ACTIVITY_LOG_NS = "@ns:com:here:xyz:log";

  public Properties() {
    xyzNamespace = new XyzNamespace();
  }

  @JsonProperty(XYZ_NAMESPACE)
  private @NotNull XyzNamespace xyzNamespace;

  /**
   * Returns the XYZ namespace, guaranteed to be always present. If parsing a feature without such
   * property, an empty one will be created.
   *
   * @return The namespace.
   */
  public @NotNull XyzNamespace getXyzNamespace() {
    return xyzNamespace;
  }

  /**
   * Sets the XYZ namespace.
   *
   * @param xyzNamespace The namespace.
   */
  public void setXyzNamespace(@NotNull XyzNamespace xyzNamespace) {
    this.xyzNamespace = xyzNamespace;
  }

  @JsonProperty(XYZ_ACTIVITY_LOG_NS)
  @JsonInclude(Include.NON_NULL)
  private @Nullable XyzActivityLog xyzActivityLog;

  /**
   * Returns the activity log namespace.
   *
   * @return The activity log namespace; if any.
   */
  public @Nullable XyzActivityLog getXyzActivityLog() {
    return xyzActivityLog;
  }

  /**
   * Sets the activity log namespace.
   *
   * @param ns The namespace.
   */
  public void setXyzActivityLog(@Nullable XyzActivityLog ns) {
    this.xyzActivityLog = ns;
  }

  /**
   * Removes the activity log namespace.
   *
   * @return The activity log namespace; if there was any.
   */
  public @Nullable XyzActivityLog removeActivityLog() {
    final XyzActivityLog xyzActivityLog = this.xyzActivityLog;
    this.xyzActivityLog = null;
    return xyzActivityLog;
  }
}
