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

import com.here.xyz.connectors.NotificationParams;
import com.here.xyz.connectors.ProcessorConnector;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

public class FailingPreProcessor extends ProcessorConnector {
    private static AtomicLong l = new AtomicLong();

    @Override
    protected ModifyFeaturesEvent processModifyFeatures(ModifyFeaturesEvent event, NotificationParams notificationParams) {
        if (event.getFailed() == null) {
            event.setFailed(new LinkedList<>());
        }
        event.setInsertFeatures(null);
        Long position = l.getAndIncrement();
        event.getFailed().add(new FeatureCollection.ModificationFailure().withId(position.toString()).withPosition(position).withMessage("" + new Date().getTime()));
        return event;
    }
}
