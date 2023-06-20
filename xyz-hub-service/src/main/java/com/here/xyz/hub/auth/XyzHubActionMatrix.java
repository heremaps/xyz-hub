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

package com.here.xyz.hub.auth;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.hub.Connector;
import com.here.naksha.lib.core.models.hub.Space;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class XyzHubActionMatrix extends ActionMatrix {

  public static final String READ_FEATURES = "readFeatures";
  public static final String CREATE_FEATURES = "createFeatures";
  public static final String UPDATE_FEATURES = "updateFeatures";
  public static final String DELETE_FEATURES = "deleteFeatures";
  public static final String MANAGE_SPACES = "manageSpaces";
  @Deprecated
  public static final String ADMIN_SPACES = "adminSpaces";
  public static final String ACCESS_CONNECTORS = "accessConnectors";
  public static final String MANAGE_CONNECTORS = "manageConnectors";
  public static final String MANAGE_PACKAGES = "managePackages";
  @Deprecated
  public static final String USE_CAPABILITIES = "useCapabilities";
  @Deprecated
  public static final String USE_ADMIN_CAPABILITIES = "useAdminCapabilities";

  /**
   * Used when the client tries to read features from a space.
   *
   * @param space the space being the target of the action.
   */
  public void readFeatures(@NotNull Space space) {
    addAction(READ_FEATURES, XyzHubAttributeMap.ofSpace(space));
  }

  /**
   * Used when the client tries to create features in a space.
   *
   * @param space the space being the target of the action.
   */
  public void createFeatures(@NotNull Space space) {
    addAction(CREATE_FEATURES, XyzHubAttributeMap.ofSpace(space));
  }

  /**
   * Used when the client tries to update features in a space.
   *
   * @param space the space being the target of the action.
   */
  public void updateFeatures(@NotNull Space space) {
    addAction(UPDATE_FEATURES, XyzHubAttributeMap.ofSpace(space));
  }

  /**
   * Used when the client tries to delete features from a space.
   *
   * @param space the space being the target of the action.
   */
  public void deleteFeatures(@NotNull Space space) {
    addAction(DELETE_FEATURES, XyzHubAttributeMap.ofSpace(space));
  }

  /**
   * Tests whether this access matrix grants the right to list the given space.
   *
   * @param space the space being the target of the action.
   * @return true if the client allowed to list (see) the basic space declaration (without {@link Space#params}; false otherwise.
   */
  public boolean mayReadySpaceWithoutParams(@NotNull Space space) {
    // TODO: We may list all spaces for which we either have the READ_FEATURES action or for which we have the MANAGE_SPACES right!
    return true;
  }

  /**
   * Used when the client tries to read a full space specification, including the {@link Space#params}.
   *
   * @param space the space being the target of the action.
   */
  public void readSpace(@NotNull Space space) {
    addAction(MANAGE_SPACES, XyzHubAttributeMap.ofSpace(space));
  }

  /**
   * Used when the client tries to create a space.
   *
   * @param space the space being the target of the action.
   * @throws XyzErrorException if the space uses connectors that are unknown (do not exist).
   */
  public void createSpace(@NotNull Space space) throws XyzErrorException {
    addAction(MANAGE_SPACES, XyzHubAttributeMap.ofSpace(space));

    // ACCESS_CONNECTORS right is needed to add a connector to the space.
    final List<@NotNull String> connectorIds = space.getConnectorIds();
    if (connectorIds != null) {
      for (final @NotNull String connectorId : connectorIds) {
        addAction(ACCESS_CONNECTORS, XyzHubAttributeMap.ofConnectorById(connectorId));
      }
    }

    // MANAGE_PACKAGES right is needed to add the space to a packages.
    final List<@NotNull String> packages = space.getPackages();
    if (packages != null) {
      for (final @NotNull String packageId : packages) {
        addAction(MANAGE_PACKAGES, XyzHubAttributeMap.ofPackage(packageId));
      }
    }
  }

  /**
   * Used when the client tries to update a space.
   *
   * @param _new the space being the target of the action.
   * @param _old the previous state of the space.
   **/
  public void updateSpace(@NotNull Space _old, @NotNull Space _new) throws XyzErrorException {
    // Client need the right to modify the old and the new space state.
    addAction(MANAGE_SPACES, XyzHubAttributeMap.ofSpace(_old));
    addAction(MANAGE_SPACES, XyzHubAttributeMap.ofSpace(_new));

    // ACCESS_CONNECTORS right is needed to add a connector to the space.
    // MANAGE_SPACES includes the right to remove a connector.
    final List<@NotNull String> newConnectorIds = _new.getConnectorIds();
    final List<@NotNull String> oldConnectorIds = _old.getConnectorIds();
    if (newConnectorIds != null) {
      for (final @NotNull String new_connectorId : newConnectorIds) {
        if (oldConnectorIds==null || !oldConnectorIds.contains(new_connectorId)) {
          addAction(ACCESS_CONNECTORS, XyzHubAttributeMap.ofConnectorById(new_connectorId));
        }
      }
    }

    // MANAGE_PACKAGES right is needed to add the space to a packages.
    // MANAGE_SPACES includes the right to remove the space from a package.
    final List<@NotNull String> newPackages = _new.getPackages();
    final List<@NotNull String> oldPackages = _old.getPackages();
    for (final @NotNull String newPackageId : newPackages) {
      if (!oldPackages.contains(newPackageId)) {
        addAction(MANAGE_PACKAGES, XyzHubAttributeMap.ofPackage(newPackageId));
      }
    }
  }

  /**
   * Used when the client tries to update a space.
   *
   * @param space the space being the target of the action.
   */
  public void deleteSpace(@NotNull Space space) {
    addAction(MANAGE_SPACES, XyzHubAttributeMap.ofSpace(space));
  }

  /**
   * Used when the client tries to read a connector specification.
   *
   * @param connector the connector being the target of the action.
   */
  public void readConnector(@NotNull Connector connector) {
    addAction(MANAGE_CONNECTORS, XyzHubAttributeMap.ofConnector(connector));
  }

  /**
   * Used when the client tries to create a connector.
   *
   * @param connector the connector being the target of the action.
   */
  public void createConnector(@NotNull Connector connector) {
    addAction(MANAGE_CONNECTORS, XyzHubAttributeMap.ofConnector(connector));

    // MANAGE_PACKAGES right is needed to add the connector to a packages.
    for (final @NotNull String packageId : connector.getPackages()) {
      addAction(MANAGE_PACKAGES, XyzHubAttributeMap.ofPackage(packageId));
    }
  }

  /**
   * Used when the client tries to update a connector.
   *
   * @param _old the previous connector state.
   * @param _new the connector being the target of the action.
   */
  public void updateConnector(@NotNull Connector _old, @NotNull Connector _new) {
    addAction(MANAGE_CONNECTORS, XyzHubAttributeMap.ofConnector(_old));
    addAction(MANAGE_CONNECTORS, XyzHubAttributeMap.ofConnector(_new));

    // MANAGE_CONNECTORS includes the right to remove the connector from a package.
    // MANAGE_PACKAGES right is needed to add the connector to a packages.
    final List<@NotNull String> newPackages = _new.getPackages();
    final List<@NotNull String> oldPackages = _old.getPackages();
    for (final @NotNull String newPackageId : _new.getPackages()) {
      if (oldPackages.contains(newPackageId)) {
        addAction(MANAGE_PACKAGES, XyzHubAttributeMap.ofPackage(newPackageId));
      }
    }
  }

  /**
   * Used when the client tries to delete a connector.
   *
   * @param connector the connector being the target of the action.
   */
  public void deleteConnector(@NotNull Connector connector) {
    addAction(MANAGE_CONNECTORS, XyzHubAttributeMap.ofConnector(connector));
  }

  // --------------------------------------------------------------------------------------------------------------------------------

  @Deprecated
  public XyzHubActionMatrix readFeatures(final AttributeMap attributesMap) throws NullPointerException {
    addAction(READ_FEATURES, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix createFeatures(final AttributeMap attributesMap) throws NullPointerException {
    addAction(CREATE_FEATURES, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix updateFeatures(final AttributeMap attributesMap) throws NullPointerException {
    addAction(UPDATE_FEATURES, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix deleteFeatures(final AttributeMap attributesMap) throws NullPointerException {
    addAction(DELETE_FEATURES, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix manageSpaces(final AttributeMap attributesMap) throws NullPointerException {
    addAction(MANAGE_SPACES, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix adminSpaces(final AttributeMap attributesMap) throws NullPointerException {
    addAction(ADMIN_SPACES, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix accessConnectors(final AttributeMap attributesMap) throws NullPointerException {
    addAction(ACCESS_CONNECTORS, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix manageConnectors(final AttributeMap attributesMap) throws NullPointerException {
    addAction(MANAGE_CONNECTORS, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix useCapabilities(final AttributeMap attributesMap) throws NullPointerException {
    addAction(USE_CAPABILITIES, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix useAdminCapabilities(final AttributeMap attributesMap) throws NullPointerException {
    addAction(USE_ADMIN_CAPABILITIES, attributesMap);
    return this;
  }

  @Deprecated
  public XyzHubActionMatrix managePackages(final AttributeMap attributesMap) throws NullPointerException {
    addAction(MANAGE_PACKAGES, attributesMap);
    return this;
  }
}
