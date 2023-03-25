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

import com.here.xyz.Typed;
import com.here.xyz.connectors.ListenerConnector;
import com.here.xyz.connectors.NotificationParams;
import com.here.xyz.events.Event;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;

@SuppressWarnings("unused")
public class MockedListener extends ListenerConnector {

  private static ThrowingBiConsumer<Event, NotificationParams> requestHandler;
  private static ThrowingBiConsumer<Typed, NotificationParams> responseHandler;

  public static void setRequestHandler(ThrowingBiConsumer<Event, NotificationParams> requestHandler) {
    MockedListener.requestHandler = requestHandler;
  }

  public static void setResponseHandler(ThrowingBiConsumer<Typed, NotificationParams> responseHandler) {
    MockedListener.responseHandler = responseHandler;
  }

  // ... to be continued when needed

  @Override
  protected void processModifyFeatures(ModifyFeaturesEvent event, NotificationParams notificationParams) throws Exception {
    if (requestHandler != null) {
      requestHandler.accept(event, notificationParams);
    }
  }

  @Override
  protected void processSearchForFeatures(FeatureCollection response, NotificationParams notificationParams) throws Exception {
    if (responseHandler != null) {
      responseHandler.accept(response, notificationParams);
    }
  }

  @FunctionalInterface
  public interface ThrowingBiConsumer<T, U> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     */
    void accept(T t, U u) throws Exception;
  }
}
