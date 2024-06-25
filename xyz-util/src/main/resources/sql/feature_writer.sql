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
CREATE OR REPLACE FUNCTION write_features(input_features TEXT, author TEXT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_feature(input_feature JSONB, base_version BIGINT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN)
    RETURNS JSONB AS $BODY$

    if(input_feature.properties['@ns:com:here:xyz'].deleted == true)
       return plv8.find_function('delete_feature(jsonb, boolean)')
            (input_feature, base_version);
    }
    return plv8.find_function('write_row(jsonb, bigint, text, text, text ,text ,boolean)')
            (input_feature, base_version, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial);

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError
 */
CREATE OR REPLACE FUNCTION delete_feature(input_feature JSONB, base_version BIGINT,
    on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN) RETURNS JSONB AS $BODY$

    if(context == "DEFAULT"){

    }else{
       //NULL - EXTENSION
        if(context("historyEnabled"))
            return plv8.find_function('write_row(jsonb, bigint, text, text, text, text, boolean)')
                (input_feature, base_version, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial);
        return plv8.find_function('delete_row(jsonb, text, text, boolean)')(input_feature, base_version, on_version_conflict);
    }

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_row(input_feature JSONB, base_version BIGINT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN)
    RETURNS JSONB AS $BODY$

    if(context("historyEnabled"))on_exists TEXT, on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict
       return plv8.find_function('write_row_with_history(jsonb, bigint, text, text, text ,text ,boolean)')
            (input_feature, base_version, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial);
    return plv8.find_function('write_row_without_history(jsonb, text, text, boolean)')
            (input_feature, on_exists, on_not_exists, is_partial);
$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_row_with_history(input_feature JSONB, base_version BIGINT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_row_without_history(input_feature JSONB, on_exists TEXT,
    on_not_exists TEXT, is_partial BOOLEAN) RETURNS JSONB AS $BODY$

    //TODO on_version_conflict is missing in method signature
    if(on_version_conflict == null){
        if(is_partial){
            headFeature = plv8.find_function('load_feature(text)')(id);
            //TODO input_features has to be enriched already!
            plv8.find_function("patch(jsonb, jsonb)")(headFeature, input_feature);
        }

        let sql = "INSERT INTO \""+ context("schema") + "\".\"" + context("table") + "\""
           + "(id, version, operation, author, jsondata, geo) VALUES ($1, $2, $3, $4, $5, ST_Force3D(ST_GeomFromGeoJSON($6)))";

        let plan = plv8.prepare(sql, ['TEXT','BIGINT','CHAR','TEXT','JSONB','JSONB']);
        try{
            let cnt = plan.execute(input_feature.id,
                input_feature.properties['@ns:com:here:xyz'].version,
                //TODO set version operation
                input_feature.properties['@ns:com:here:xyz'].operation == undefined ? 'I' : input_feature.properties['@ns:com:here:xyz'].operation,
                input_feature.properties['@ns:com:here:xyz'].author,
                //TODO - check if nessecary
                plv8.find_function("clean_feature(jsonb)")(jsondata),
                input_feature.geometry);
        }catch(e){
            //UNIQUE VIOLATION
            if(e.sqlerrcode != undefined && e.sqlerrcode == '23505')
                //Update feature
            else
                plv8.elog(ERROR, e);
        }        
    }
	plan.free();
$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError
 * TODO: do we need the payload of the feature as return?
 */
CREATE OR REPLACE FUNCTION delete_row(input_feature JSONB, base_version BIGINT, on_version_conflict TEXT)
    RETURNS VOID AS $BODY$

    let cnt;
    //base_version get provided from user (extend api endpoint)
    plv8.elog(NOTICE, 'Delete id=', input_feature.id,);
    let sql = "DELETE FROM \""+ context("schema") + "\".\"" + context("table") + "\"" WHERE id = $1"";
    let plan = plv8.prepare(sql, ['TEXT']);

	if(on_version_conflict == null){
		let plan = plv8.prepare(sql, ['TEXT']);
		cnt = plan.execute(input_feature.id);
    }else{
        if(base_version == 0){
            sql = 'AND next_version = max_bigint();'
            cnt = plan.execute(input_feature.id);
        }else{
            sql = 'AND version = $2;'
            plan = plv8.prepare(sql, ['TEXT','BIGINT']);
            cnt = plan.execute(input_feature.id, base_version);
        }

        if(cnt == 0){
            plv8.elog(NOTICE, 'HandleConflict for id=', input_feature.id,);
            //handleDeleteVersionConflict
        }
    }
    plan.free();
$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError
 */
CREATE OR REPLACE FUNCTION handle_version_conflict(input_feature JSONB, base_version BIGINT,
    on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN, headFeature JSONB)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws MergeConflictError
 */
CREATE OR REPLACE FUNCTION handle_merge_conflict(input_feature JSONB, base_version BIGINT,
    on_merge_conflict TEXT, is_partial BOOLEAN)
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

CREATE OR REPLACE FUNCTION patch(target JSONB, input_diff JSONB) RETURNS JSONB AS $BODY$
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

CREATE OR REPLACE FUNCTION enrich_feature(input_feature JSONB, version BIGINT, author TEXT)
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
    base_version BIGINT, on_merge_conflict TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;