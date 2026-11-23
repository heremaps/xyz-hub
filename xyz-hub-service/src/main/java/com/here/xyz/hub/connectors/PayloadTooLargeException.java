/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.connectors;

import com.here.xyz.util.service.errors.DetailedHttpException;
import java.util.Map;

/**
 * The connector refused to build a response that large ({@code PAYLOAD_TO_LARGE}). The client gets the same 513 (E318541) for each
 * connector. The own type only keeps the {@code errorDetails} of the connector's error, which the client does not get, e.g. for the
 * Iceberg connector which limit was hit, see {@link com.here.xyz.hub.task.IcebergReadRouting}.
 */
public class PayloadTooLargeException extends DetailedHttpException {

  private static final long serialVersionUID = 1L;

  /**
   * The {@code errorDetails} of the connector's error response, or null
   */
  public final transient Map<String, Object> connectorDetails;

  public PayloadTooLargeException(Map<String, Object> connectorDetails) {
    super("E318541");
    this.connectorDetails = connectorDetails;
  }
}
