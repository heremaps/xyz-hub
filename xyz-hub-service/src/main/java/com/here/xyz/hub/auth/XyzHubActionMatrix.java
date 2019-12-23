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

@SuppressWarnings("serial")
public class XyzHubActionMatrix extends ActionMatrix {

  public static final String READ_FEATURES = "readFeatures";
  public static final String CREATE_FEATURES = "createFeatures";
  public static final String UPDATE_FEATURES = "updateFeatures";
  public static final String DELETE_FEATURES = "deleteFeatures";
  public static final String MANAGE_SPACES = "manageSpaces";
  public static final String ADMIN_SPACES = "adminSpaces";
  public static final String ACCESS_CONNECTORS = "accessConnectors";
  public static final String MANAGE_PACKAGES = "managePackages";
  public static final String USE_CAPABILITIES = "useCapabilities";
  public static final String USE_ADMIN_CAPABILITIES = "useAdminCapabilities";


  public XyzHubActionMatrix readFeatures(final AttributeMap attributesMap) throws NullPointerException {
    addAction(READ_FEATURES, attributesMap);
    return this;
  }

  public XyzHubActionMatrix createFeatures(final AttributeMap attributesMap) throws NullPointerException {
    addAction(CREATE_FEATURES, attributesMap);
    return this;
  }

  public XyzHubActionMatrix updateFeatures(final AttributeMap attributesMap) throws NullPointerException {
    addAction(UPDATE_FEATURES, attributesMap);
    return this;
  }

  public XyzHubActionMatrix deleteFeatures(final AttributeMap attributesMap) throws NullPointerException {
    addAction(DELETE_FEATURES, attributesMap);
    return this;
  }

  public XyzHubActionMatrix manageSpaces(final AttributeMap attributesMap) throws NullPointerException {
    addAction(MANAGE_SPACES, attributesMap);
    return this;
  }

  public XyzHubActionMatrix adminSpaces(final AttributeMap attributesMap) throws NullPointerException {
    addAction(ADMIN_SPACES, attributesMap);
    return this;
  }

  public XyzHubActionMatrix accessConnectors(final AttributeMap attributesMap) throws NullPointerException {
    addAction(ACCESS_CONNECTORS, attributesMap);
    return this;
  }

  public XyzHubActionMatrix useCapabilities(final AttributeMap attributesMap) throws NullPointerException {
    addAction(USE_CAPABILITIES, attributesMap);
    return this;
  }

  public XyzHubActionMatrix useAdminCapabilities(final AttributeMap attributesMap) throws NullPointerException {
    addAction(USE_ADMIN_CAPABILITIES, attributesMap);
    return this;
  }

  public XyzHubActionMatrix managePackages(final AttributeMap attributesMap) throws NullPointerException {
    addAction(MANAGE_PACKAGES, attributesMap);
    return this;
  }
}
