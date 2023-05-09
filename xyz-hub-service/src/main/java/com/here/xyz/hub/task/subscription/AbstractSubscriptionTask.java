/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.hub.task.subscription;

import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.events.AbstractSubscriptionEvent;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.NakshaTask;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All tasks related to connectors.
 */
public abstract class AbstractSubscriptionTask<EVENT extends AbstractSubscriptionEvent> extends NakshaTask<EVENT> {

  protected AbstractSubscriptionTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public void initEventFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);
  }

}