/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

require("../../plv8");
require("../../../../main/resources/sql/Exception");
require("../../../../main/resources/sql/FeatureWriter");
require("../../../../main/resources/sql/DatabaseWriter");
const FeatureWriter = plv8.FeatureWriter;
const DatabaseWriter = plv8.DatabaseWriter;


global.queryContext = () => ({
  schema: "public",
  table: "composite-export-space-ext-ext",
  extendedTable: "composite-export-space-ext",
  extendedTableL2: "composite-export-space",
  context: "DEFAULT",
  historyEnabled: false
});

class TestFeatureWriter {

  inputFeature = {
    "id": "id4",
    "geometry": {
      "type": "Point",
      "coordinates": [
        8,
        50
      ]
    },
    "properties": {
      "firstName": "Alice",
      "age": 35,
      "@ns:com:here:xyz": {
        deleted: true
      }
    }
  };

  modification = [{
    updateStrategy: {
      onExists: "DELETE",
      onNotExists: "ERROR",
      onVersionConflict: null,
      onMergeConflict: null
    },
    featureIds: ["Q12345678"],
    partialUpdates: false,
    featureHooks: null
  }];

  run() {
    let result = FeatureWriter.writeFeature(this.inputFeature, null, "REPLACE", "CREATE", null, null, false, null);
    console.log("Returned result from FeatureWriter: ", result);
  }

}

new TestFeatureWriter().run();