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

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_features(inputFeatures TEXT, author TEXT, onExists TEXT,
    onNotExists TEXT, onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_feature(inputFeature JSONB, baseVersion TEXT, onExists TEXT,
    onNotExists TEXT, onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError
 */
CREATE OR REPLACE FUNCTION delete_feature(inputFeature JSONB, baseVersion TEXT,
    onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_row(inputFeature JSONB, baseVersion TEXT, onExists TEXT,
    onNotExists TEXT, onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_row_with_history(inputFeature JSONB, baseVersion TEXT, onExists TEXT,
    onNotExists TEXT, onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_row_without_history(inputFeature JSONB, onExists TEXT,
    onNotExists TEXT, isPartial TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError
 */
CREATE OR REPLACE FUNCTION delete_row(inputFeature JSONB, baseVersion TEXT, onVersionConflict TEXT)
    RETURNS VOID AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError
 */
CREATE OR REPLACE FUNCTION handle_version_conflict(inputFeature JSONB, baseVersion TEXT,
    onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT, headFeature JSONB)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws MergeConflictError
 */
CREATE OR REPLACE FUNCTION handle_merge_conflict(inputFeature JSONB, baseVersion TEXT,
    onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

CREATE OR REPLACE FUNCTION load_feature(id TEXT, version BIGINT = -1)
    RETURNS JSONB AS $BODY$
    /**TODO: Check if we need a check on operation.*/
    if(id == null)
      return null;

    let sql = "SELECT id, version, author, jsondata, geo::JSONB "  //operation,next_version,i
        + "FROM \""+ context("schema") + "\".\"" + context("table") + "\"";

    let res;
    if(version == -1){
      /** next_version + operation supports head retrival if we have multiple versions */
      sql += 'WHERE id = $1 '
          +'AND next_version = max_bigint() '
          +'AND operation != $2';
      let plan = plv8.prepare(sql, ['TEXT','CHAR']);
      res = plan.execute(id,'D');
      plan.free();
    }else{
      sql += 'WHERE id = $1 '
          +'AND version = $2 '
          +'AND operation != $3';
      let plan = plv8.prepare(sql, ['TEXT','BIGINT','CHAR']);
      res = plan.execute(id, version, 'D');
      plan.free();
    }

    if(res.length == 0){
      return null;
    }else if(res.length == 1){
      feature = res[0].jsondata;
      feature.id = res[0].id;
      feature.geometry = res[0].geo;
      feature.properties['@ns:com:here:xyz'].version = res[0].version;
      feature.properties['@ns:com:here:xyz'].author = res[0].author;
      if(feature.geometry != null)
        delete feature.geometry.crs;
    }else{
      plv8.elog(ERROR, 'Found two Features with same id!');
    }

    return feature;
$BODY$ LANGUAGE plv8 IMMUTABLE;

CREATE OR REPLACE FUNCTION diff(minuend JSONB, subtrahend JSONB) RETURNS JSONB AS $BODY$
    let diff = {};

    for (let key in subtrahend) {
      if (subtrahend.hasOwnProperty(key)) {
        if (typeof subtrahend[key] === 'object' && !Array.isArray(subtrahend[key]) && subtrahend[key] !== null) {
          // Recursively diff nested objects
          let nestedDiff= plv8.find_function('diff(jsonb, jsonb)')(minuend[key] || {}, subtrahend[key]);

          if (Object.keys(nestedDiff).length > 0) {
            diff[key] = nestedDiff;
          }
        } else if (minuend[key] !== subtrahend[key]) {
          // Add changed or new properties
          diff[key] = subtrahend[key];
        }
      }
    }

    // Check for removed properties
    for (let key in minuend) {
      if (minuend.hasOwnProperty(key) && !subtrahend.hasOwnProperty(key)) {
        diff[key] = null;
      }
    }

    return diff;
$BODY$ LANGUAGE plv8 IMMUTABLE;

CREATE OR REPLACE FUNCTION patch(target JSONB, inputDiff JSONB) RETURNS JSONB AS $BODY$
    for (let key in input_diff) {
      if (input_diff.hasOwnProperty(key)) {
        if (input_diff[key] === null) {
          delete target[key];
        } else if (typeof input_diff[key] === 'object' && !Array.isArray(input_diff[key]) && input_diff[key] !== null) {
          if (!target[key]) {
            target[key] = {};
          }
          plv8.elog(NOTICE,'patch [',key,'] -> ', JSON.stringify(target[key]),'-',JSON.stringify(input_diff[key]));
          target[key] = plv8.find_function('patch(jsonb, jsonb)')(target[key], input_diff[key]);
        } else {
          target[key] = input_diff[key];
        }
      }
    }
    return target;
$BODY$ LANGUAGE plv8 IMMUTABLE;

CREATE OR REPLACE FUNCTION enrich_feature(inputFeature JSONB, version BIGINT, author TEXT)
    RETURNS JSONB AS $BODY$
    if(input_feature == null)
      plv8.elog(ERROR, 'Feature is not allowed to be null!');
    if(version == null)
      plv8.elog(ERROR, 'Version is not allowed to be null!');
    if(input_feature.id == undefined)
      input_feature.id = Math.random().toString(36).slice(2, 10) ;
    if(input_feature.type == undefined)
      input_feature.type = 'Feature';
    if(author == null)
      author = 'ANOYMOUS';

    if(input_feature.properties == undefined)
      input_feature.properties = {};
    if(input_feature.properties['@ns:com:here:xyz'] == undefined)
      input_feature.properties['@ns:com:here:xyz'] = {};

    input_feature.properties['@ns:com:here:xyz'].createdAt = (input_feature.properties['@ns:com:here:xyz'].createdAt == undefined) ? Date.now() : (input_feature.properties['@ns:com:here:xyz'].createdAt);
    input_feature.properties['@ns:com:here:xyz'].updatedAt = Date.now();
    input_feature.properties['@ns:com:here:xyz'].version = version;
    input_feature.properties['@ns:com:here:xyz'].author = author;

    return input_feature;
$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws MergeConflictError
 */
CREATE OR REPLACE FUNCTION merge_changes(headFeature JSONB, inputDiff JSONB, headDiff JSONB,
    baseVersion BIGINT, onMergeConflict TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;