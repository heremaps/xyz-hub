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

package com.here.xyz.connectors.mocks;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.NotificationParams;
import com.here.xyz.connectors.ProcessorConnector;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.geojson.implementation.XyzError;

@SuppressWarnings("unused")
public class MockedProcessor extends ProcessorConnector {

  private static Mode mode;

  public static void setMode(Mode mode) {
    MockedProcessor.mode = mode;
  }

  @Override
  protected ModifySpaceEvent processModifySpace(ModifySpaceEvent event, NotificationParams notificationParams) throws Exception {
    switch (mode) {
      case SUCCESS:
        return event;
      case ERROR:
        throw new ErrorResponseException(this.streamId, XyzError.EXCEPTION, "Wrong argument value provided.");
      default:
        throw new ErrorResponseException(this.streamId, XyzError.EXCEPTION, "No execution mode defined for MockedProcessor.");
    }
  }

  public enum Mode {
    ERROR,
    SUCCESS
  }
}
