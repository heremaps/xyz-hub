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


global.queryContext = () => TestFeatureWriter.queryContext;

class TestFeatureWriter {
  static queryContext = {
    schema: "public",
    tables: ["composite-export-space", "composite-export-space-ext", "composite-export-space-ext-ext"],
    context: "DEFAULT",
    historyEnabled: false
  };

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

  sqlQueryJson = {
    "namedParameters" : {
      "jsonInput" : "[{\"updateStrategy\":{\"onExists\":\"REPLACE\",\"onNotExists\":\"CREATE\",\"onVersionConflict\":\"MERGE\",\"onMergeConflict\":\"ERROR\"},\"featureData\":{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"id\":\"f1\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,0.0,0.0]},\"properties\":{\"@ns:com:here:xyz\":{\"createdAt\":1755901967515,\"updatedAt\":1755901967698,\"version\":2},\"B\":\"valueB\"}}]},\"partialUpdates\":false}]",
      "version" : 3,
      "author" : "Test Author",
      "returnResult" : false
    },
    "queryId" : "75b29e65-a04b-4da2-ac40-e8054a247dab",
    "context" : {
      "schema" : "public",
      "historyEnabled" : true,
      "tables" : [ "BMMerge", "BMMerge_0_2_2" ],
      "tableBaseVersions" : [ 0, 2 ],
      "baseVersion" : 1,
      "batchMode" : true
    },
    "text" : "SELECT write_features(#{jsonInput}, 'Modifications', #{author}, #{returnResult}, #{version})"
  }

  runFromSqlQueryJson() {
    TestFeatureWriter.queryContext = this.sqlQueryJson.context;
    let result = FeatureWriter.writeFeatureModifications(JSON.parse(this.sqlQueryJson.namedParameters.modifications || this.sqlQueryJson.namedParameters.featureModificationList || this.sqlQueryJson.namedParameters.jsonInput), this.sqlQueryJson.namedParameters.author, this.sqlQueryJson.namedParameters.version);
    console.log("Returned result from FeatureWriter: ", result);
  }

  run() {
    let result = FeatureWriter.writeFeature(this.inputFeature, null, "REPLACE", "CREATE", null, null, false, null);
    console.log("Returned result from FeatureWriter: ", result);
  }

}

// new TestFeatureWriter().run();
new TestFeatureWriter().runFromSqlQueryJson();