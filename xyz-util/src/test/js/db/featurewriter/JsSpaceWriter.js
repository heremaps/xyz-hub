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

require("../../plv8");
require("../../../../main/resources/sql/Exception");
require("../../../../main/resources/sql/FeatureWriter");
const FeatureWriter = plv8.FeatureWriter;
const XYZ_NS = "@ns:com:here:xyz";

class JsSpaceWriter {

  schema = "public";
  spaceId;
  composite;

  constructor(composite, testSuiteName) {
    this.spaceId = testSuiteName;
    this.composite = composite;
  }

  createSpaceResources() {
    //TODO:
  }

  cleanSpaceResources() {
    this.dropSpace(this.spaceId);
    if (this.composite)
      this.dropSpace(this.superSpaceId);
  }

  dropSpace(table) {
    //TODO:
  }

  writeFeature(feature, author, onExists, onNotExists, onVersionConflict, onMergeConflict, partial, spaceContext, historyActive, expectedError) {
    return this.writeFeatureModification({
      updateStrategy: {
        onExists:  onExists,
        onNotExists:  onNotExists,
        onVersionConflict: onVersionConflict,
        onMergeConflict: onMergeConflict
      },
      featureData: {type: "FeatureCollection", features: [feature]},
      partialUpdates: partial
    }, author, spaceContext, historyActive, expectedError);
  }

  writeFeatureModification(modification, author, spaceContext, historyActive, expectedError) {
    let queryContext = {
      schema: this.schema,
      table: this.spaceId,
      context: spaceContext,
      historyEnabled: historyActive
    };
    if (this.composite)
      queryContext.extendedTable = this.superSpaceId;
    global.queryContext = () => queryContext;

    try {
      return FeatureWriter.writeFeatureModifications([modification], author);
    }
    catch (e) {
      throw e;
      //TODO: Check expected error
    }
  }

  getFeature(spaceContext) {
    return this.toFeature(this.getFeatureRow(spaceContext));
  }

  toFeature(row) {
    if (row == null)
      return null;

    let feature = row.jsondata;
    feature.properties[XYZ_NS] = {
      ...feature.properties[XYZ_NS],
      version: row.version,
      author: row.author
    };
    feature.geometry = row.geo;
    return feature;
  }

  getFeatureRow(spaceContext) {
    //TODO:
  }

  getRowCount(spaceContext) {
    //TODO:
  }

  getLastUsedFeatureOperation(spaceContext) {
    //TODO:
  }

  checkNotExistingFeature(id) {
    //TODO:
  }

  get superSpaceId() {
    if (!this.composite)
      throw new IllegalArgumentException(this.spaceId + " is not a composite space");
    return this.spaceId + "_super";
  }
}

new TestFeatureWriter().run();