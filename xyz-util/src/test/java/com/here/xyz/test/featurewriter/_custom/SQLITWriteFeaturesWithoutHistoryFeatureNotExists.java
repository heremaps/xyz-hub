/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.test.featurewriter._custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import org.junit.Test;

public class SQLITWriteFeaturesWithoutHistoryFeatureNotExists extends SQLITWriteFeaturesBase {
    private Feature f1;

    public SQLITWriteFeaturesWithoutHistoryFeatureNotExists() throws JsonProcessingException {
        super(false);
        f1 = XyzSerializable.deserialize("""
            { "type":"Feature",
              "id":"id1",
              "geometry":{"type":"Point","coordinates":[8.0,50.0]},
              "properties":{"firstName":"Alice","age":35}
            }
            """, Feature.class);
    }
    //********************** Feature not exists (OnVersionConflict deactivated) *******************************/
    @Test
    public void writeToNotExistingFeature_OnNotExistsCREATE() throws Exception {
        //Insert Feature
        writeFeature(f1, DEFAULT_AUTHOR, null, OnNotExists.CREATE,
                null, null, false, SpaceContext.EXTENSION, false, null);
        checkExistingFeature(f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
    }

    @Test
    public void writeToNotExistingFeature_OnNotExistsERROR() throws Exception {
        writeFeature(f1, DEFAULT_AUTHOR, null, OnNotExists.ERROR,
                null, null, false, SpaceContext.EXTENSION, false, SQLError.FEATURE_NOT_EXISTS);
        checkNotExistingFeature(DEFAULT_FEATURE_ID);
    }

    @Test
    public void writeToNotExistingFeature_OnNotExistsRETAIN() throws Exception {
        writeFeature(f1, DEFAULT_AUTHOR, null, OnNotExists.RETAIN,
                null, null, false, SpaceContext.EXTENSION, false, null);
        checkNotExistingFeature(f1.getId());
    }

    //********************** Feature not exists (OnVersionConflict.REPLACE) *******************************/
    @Test
    public void writeToNotExistingFeature_WithConflictHandling_WithoutBaseVersion() throws Exception {
        writeFeature(f1, DEFAULT_AUTHOR, null, OnNotExists.CREATE, OnVersionConflict.REPLACE, null,
                false, SpaceContext.EXTENSION, false, SQLError.ILLEGAL_ARGUMENT);

        checkNotExistingFeature(f1.getId());
    }

    @Test
    public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsCREATE() throws Exception {
        //Add version
        f1.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1L));
        writeFeature(f1, DEFAULT_AUTHOR, null, OnNotExists.CREATE, OnVersionConflict.REPLACE, null,
                false, SpaceContext.EXTENSION, false, null);

        checkExistingFeature(f1, 1L, Long.MAX_VALUE, Operation.I, DEFAULT_AUTHOR);
    }

    @Test
    public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsERROR() throws Exception {
        //Add version
        f1.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1L));
        writeFeature(f1, DEFAULT_AUTHOR, null, OnNotExists.ERROR, OnVersionConflict.REPLACE, null,
                false, SpaceContext.EXTENSION, false, SQLError.FEATURE_NOT_EXISTS);

        checkNotExistingFeature(f1.getId());
    }

    @Test
    public void writeToNotExistingFeature_WithConflictHandling_OnNotExistsRETAIN() throws Exception {
        //Add version
        f1.getProperties().withXyzNamespace(new XyzNamespace().withVersion(1L));
        writeFeature(f1, DEFAULT_AUTHOR, null, OnNotExists.RETAIN, OnVersionConflict.REPLACE, null,
                false, SpaceContext.EXTENSION, false, null);

        checkNotExistingFeature(f1.getId());
    }
}
