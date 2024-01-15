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
package com.here.naksha.lib.view;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import java.util.ArrayList;
import java.util.List;

public class Sample {

  public static List<XyzFeatureCodec> sampleXyzResponse(int size) {
    XyzFeatureCodecFactory codecFactory = XyzFeatureCodecFactory.get();
    List<XyzFeatureCodec> returnList = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      XyzFeatureCodec codec = codecFactory.newInstance();
      codec.setFeature(new XyzFeature("id" + i));
      codec.decodeParts(true);
      returnList.add(codec);
    }
    return returnList;
  }
}
