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

package com.here.xyz.hub.connectors.test;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.NotificationParams;
import com.here.xyz.connectors.ProcessorConnector;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.responses.XyzError;

public class ErrorPreProcessor extends ProcessorConnector {
    @Override
    protected ModifyFeaturesEvent processModifyFeatures(ModifyFeaturesEvent event, NotificationParams notificationParams) throws ErrorResponseException {
        throw new ErrorResponseException(streamId, XyzError.ILLEGAL_ARGUMENT, "Test Error");
    }
}
