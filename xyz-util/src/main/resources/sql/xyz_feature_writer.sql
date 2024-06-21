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

---------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------
-- The following methods are public and may be called by software directly
-- MergeConflictResolution: ERROR, RETAIN, REPLACE
-- VersionConflictResolution: ERROR, RETAIN, REPLACE, MERGE
-- if on_version_conflict = null => conflictDetection is off
-- TODO: add E(onExists) and NE=(OnNotExists) and Context (EXTENSION,DEFAULT,NULL)
CREATE OR REPLACE FUNCTION write_feature(tbl regclass, input_feature JSONB, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN,
       author TEXT, version BIGINT) RETURNS JSONB AS $BODY$

	function loadHeadFeature(id){
		sql = 'select * from loadFeature($1,$2)';
		plan = plv8.prepare(sql, ['TEXT','TEXT']);
        return plan.execute(tbl, id)[0].loadfeature;
    }

	function enrichInput(createdAt){
		sql = 'select enrich_feature($1,$2,$3,$4)';
		plan = plv8.prepare(sql, ['JSONB','TEXT','BIGINT','BIGINT']);
        return plan.execute(input_feature, author, version, createdAt)[0].enrich_feature;
    }

	function updateFeature(feature){
		sql = 'select update_row($1, enrich_feature($2,$3,$4));';
		let plan = plv8.prepare(sql, ['TEXT','JSONB','TEXT','BIGINT']);
        return plan.execute(tbl, feature == undefined ? input_feature : feature, author, version)[0].update_row;
    }

	var sql = 'select write_row($1, enrich_feature($2,$3,$4));';
	let plan = plv8.prepare(sql, ['TEXT','JSONB','TEXT','BIGINT']);
	let cnt = plan.execute(tbl, input_feature, author, version);
	plan.free();

	if(cnt[0].write_row == 0){
		//Insert is failed..

		switch(on_version_conflict){
			case "RETAIN" :
				plv8.elog(INFO, 'RETAIN - DO NOTHING!');
				return;
            case "REPLACE" :
				if(!is_partial){
				//UPDATE ->Override feature
                    let resCnt = updateFeature();
                    plv8.elog(INFO, 'REPLACED exisiting feature!');
                }else{
					//Patch feature - if possible than UPDATE
					let headFeature = loadHeadFeature(input_feature.id);
                    let enrichedInput = enrichInput(headFeature.properties['@ns:com:here:xyz'].createdAt);
					plv8.elog(INFO, 'LOADED exisiting feature: ', JSON.stringify(headFeature), ' INPUT: ',JSON.stringify(enrichedInput));

                    sql = 'select * from patch($1,$2)';
					plan = plv8.prepare(sql, ['JSONB','JSONB']);
					let patchedFeature = plan.execute(headFeature, enrichedInput)[0].patch;

					plv8.elog(INFO, 'PATCH result: ', JSON.stringify(patchedFeature));
					updateFeature(patchedFeature);
					plv8.elog(INFO, 'PACHED exisiting!');
                }
				break;
            case "MERGE" :
				plv8.elog(INFO, 'MERGE');

                //MERGE feature - if possible than UPDATE
                loadHeadFeature(tbl, input_feature.id);
                break;
            case "ERROR" :
                default:
                plv8.elog(ERROR, 'ERROR CONFLICT (id, version, next_version)!');
            }
    }
    //TODO: what do we want to return in detail?
	plan.free();
    return null;
$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION delete_feature(tbl regclass, id TEXT, version BIGINT, on_version_conflict TEXT ) RETURNS JSONB AS $BODY$    -- History yes/no
    /**
       History yes/no
       Context extension| default | null

       Extension => with history (Insert with D)
              => w/o (real delete)
       Default  => no history (Insert with H (not exist in delta) | Update J (exists in delta))
              => history Insert with (Operation=H )

       TODO: Check why call with geo=null not works
	*/
	plv8.elog(NOTICE, 'Delete id=',id);

	let cnt;

	if(on_version_conflict == ''){
		/** w/o versioning! TODO: take composite into account */
		let sql = 'DELETE FROM '+tbl+' WHERE id = $1';
		let plan = plv8.prepare(sql, ['TEXT']);

		cnt = plan.execute(id);
		plan.free();
    }else{
		/** with versioning! TODO: take composite into account */
		/** TODO write null + check if we need version in NS (legacy history)) */
		let sql ='select write_row($1,$2,$3,$4,$5,$6,ST_GeomFromGeoJSON($7)::GEOMETRY);';
		let plan = plv8.prepare(sql, ['REGCLASS','TEXT','BIGINT','CHAR','TEXT','JSONB','JSONB']);

		cnt = plan.execute(tbl, id, version , 'D', 'author', '{}', null);
		plan.free();
    }
	return cnt;
$BODY$ LANGUAGE plv8 IMMUTABLE;
---------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------
-- The following methods are internal to this module and may not be called by software directly

CREATE OR REPLACE FUNCTION handle_version_conflict(input_feature JSONB, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION merge_changes(base_feature JSONB, input_diff JSONB, head_diff JSONB, on_merge_conflict TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION handle_merge_conflict(input_diff JSONB, head_diff JSONB, on_merge_conflict TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION write_row(input_feature JSONB, input_head JSONB) RETURNS VOID AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION write_row(tbl regclass, input_feature JSONB) RETURNS INTEGER AS $BODY$
	let sql = 'select write_row($1, $2, $3, $4, $5, $6, ST_GeomFromGeoJSON($7)::GEOMETRY)';
	let plan = plv8.prepare(sql, ['TEXT','TEXT','BIGINT','CHAR','TEXT','JSONB','JSONB']);

	let cnt = plan.execute(
		tbl,
		input_feature.id,
		input_feature.properties['@ns:com:here:xyz'].version,
		input_feature.properties['@ns:com:here:xyz'].operation == undefined ? 'I' : input_feature.properties['@ns:com:here:xyz'].operation,
		input_feature.properties['@ns:com:here:xyz'].author,
		input_feature,
		input_feature.geometry
	);
	plan.free();
    return cnt[0].write_row;
$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION write_row(tbl regclass, id TEXT, version BIGINT, operation CHAR, author TEXT, jsondata JSONB, geo GEOMETRY) RETURNS INTEGER AS $BODY$
    delete jsondata.geometry;
	if(jsondata.properties != undefined){
		delete jsondata.properties['@ns:com:here:xyz'].version;
		delete jsondata.properties['@ns:com:here:xyz'].author;
		delete jsondata.properties['@ns:com:here:xyz'].operation;
    }

	let sql = 'INSERT INTO '+tbl+' (id, version, operation, author, jsondata, geo) VALUES ($1, $2, $3, $4, $5, ST_Force3D($6))';
	let plan = plv8.prepare(sql, ['TEXT','BIGINT','CHAR','TEXT','JSONB','GEOMETRY']);
	let cnt = plan.execute(id, version, operation, author, jsondata, geo);
	plan.free();
    return cnt;
$BODY$ LANGUAGE plv8 IMMUTABLE;
--------------------------------------------------
CREATE OR REPLACE FUNCTION update_row(tbl regclass, input_feature JSONB) RETURNS INTEGER AS $BODY$
	let sql = 'select update_row($1, $2, $3, $4, $5, $6, ST_GeomFromGeoJSON($7)::GEOMETRY)';
	let plan = plv8.prepare(sql, ['TEXT','TEXT','BIGINT','CHAR','TEXT','JSONB','JSONB']);

	let cnt = plan.execute(
		tbl,
		input_feature.id,
		input_feature.properties['@ns:com:here:xyz'].version,
		input_feature.properties['@ns:com:here:xyz'].operation == undefined ? 'U' : input_feature.properties['@ns:com:here:xyz'].operation,
		input_feature.properties['@ns:com:here:xyz'].author,
		input_feature,
		input_feature.geometry
	);
	plan.free();
return cnt[0].update_row;
$BODY$ LANGUAGE plv8 IMMUTABLE;
--------------------------------------------------
CREATE OR REPLACE FUNCTION update_row(tbl regclass, id TEXT, version BIGINT, operation CHAR, author TEXT, jsondata JSONB, geo GEOMETRY) RETURNS INTEGER AS $BODY$
	delete jsondata.geometry;

    //Part already handled inside enrich_feature
	if(jsondata.properties == undefined)
		jsondata.properties = {};
	if(jsondata.properties['@ns:com:here:xyz'] == undefined)
		jsondata.properties['@ns:com:here:xyz'] = {};
    else{
            delete jsondata.properties['@ns:com:here:xyz'].version;
            delete jsondata.properties['@ns:com:here:xyz'].author;
            delete jsondata.properties['@ns:com:here:xyz'].operation;
    }

	let sql = 'UPDATE '+tbl+' SET '
			+ 'version = $1, '
			+ 'operation = $2, '
			+ 'author = $3, '
			+ "jsondata = jsonb_set($4, '{properties,@ns:com:here:xyz,createdAt}', "
			+ "  COALESCE(jsondata->'properties'->'@ns:com:here:xyz'->'createdAt', to_jsonb($5)) "
			+ '),'
			+ 'geo = ST_Force3D($6) '
			+ 'WHERE id = $7';

	let plan = plv8.prepare(sql, ['BIGINT','CHAR','TEXT','JSONB','BIGINT','GEOMETRY','TEXT']);
	//keep createdAt
	let createdAt = (jsondata.properties != null  ? jsondata.properties['@ns:com:here:xyz'].createdAt : null);
	let cnt = plan.execute(version, operation, author, jsondata, createdAt, geo, id);
	plan.free();

return cnt;
$BODY$ LANGUAGE plv8 IMMUTABLE ;
---------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------
-- Some helper methods

-- Load Feature from given table. If no version is provided the head version gets returned, otherwise
-- the feature with the specified version is getting returned. If no feature can be found, null is returned.
CREATE OR REPLACE FUNCTION loadFeature(tbl regclass, id TEXT, version BIGINT = -1) RETURNS JSONB AS $$
    /**TODO: Check if we need a check on operation.*/
    if(id == null)
        return null;

    let sql = 'SELECT id, version, author, jsondata, geo::JSONB '  //operation,next_version,i
        + 'FROM '+ tbl + ' ';

    let res;
    if(version == -1){
        /** next_version + operation supports head retrival if we have multiple versions */
        sql += 'WHERE id = $1 '
            +'AND next_version = max_bigint() '
            +'AND operation != $2';
        let plan = plv8.prepare(sql, ['TEXT','CHAR']);
        res = plan.execute(id,'D');
    }else{
        sql += 'WHERE id = $1 '
            +'AND version = $2 '
            +'AND operation != $3';
        let plan = plv8.prepare(sql, ['TEXT','BIGINT','CHAR']);
        res = plan.execute(id, version, 'D');
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

    plan.free();

    return feature;
$$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION diff(minuend JSONB, subtrahend JSONB) RETURNS JSONB AS $BODY$
    let diff = {};

    for (let key in subtrahend) {
        if (subtrahend.hasOwnProperty(key)) {
            if (typeof subtrahend[key] === 'object' && !Array.isArray(subtrahend[key]) && subtrahend[key] !== null) {
                // Recursively diff nested objects
                let nestedDiff = diffGeoJSON(minuend[key] || {}, subtrahend[key]);
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
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION patch(target JSONB, input_diff JSONB) RETURNS JSONB AS $BODY$
    for (let key in input_diff) {
        if (input_diff.hasOwnProperty(key)) {
            if (input_diff[key] === null) {
                delete target[key];
            } else if (typeof input_diff[key] === 'object' && !Array.isArray(input_diff[key]) && input_diff[key] !== null) {
                if (!target[key]) {
                    target[key] = {};
                }
                var plan = plv8.prepare('select patch($1, $2)', ['jsonb','jsonb']);
                plv8.elog(NOTICE,'patch [',key,'] -> ', JSON.stringify(target[key]),'-',JSON.stringify(input_diff[key]));
                target[key] = plan.execute(target[key], input_diff[key])[0].patch;
                plan.free();
            } else {
                target[key] = input_diff[key];
            }
        }
    }
    return target;
$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION find_conflicts(obj1 JSONB, obj2 JSONB, path TEXT) RETURNS JSONB AS $BODY$
    // Helper function to determine if a value is an object
    function isObject(obj) {
            return obj !== null && typeof obj === 'object' && !Array.isArray(obj);
    }

    // Iterate over keys of the first object
    for (const key in obj1) {
        if (obj1.hasOwnProperty(key)) {
            const currentPath = path ? (path+'.'+key) : key;
            if (obj2.hasOwnProperty(key)) {
                // If both values are objects, recurse
                if (isObject(obj1[key]) && isObject(obj2[key])) {
					var plan = plv8.prepare('select find_conflicts($1, $2, $3)', ['jsonb','jsonb','text']);
					plan.execute(obj1[key], obj2[key], currentPath);
                } else if (obj1[key] !== obj2[key]) {
                    // If values differ, throw an error
                    let details = {
                        path : currentPath,
                        value1 : obj1[key],
                        value2 : obj2[key]
                    }
				    plv8.elog(ERROR, 'Conflict ', 'cause: ', JSON.stringify(details));
                }
            }
        }
    }

    // Iterate over keys of the second object
    for (const key in obj2) {
        if (obj2.hasOwnProperty(key) && !obj1.hasOwnProperty(key)) {
            const currentPath = path ? (path+'.'+key) : key;

            // Check if key is present in obj2 but not in obj1 and if both values are objects
            if (isObject(obj2[key]) && !isObject(obj1[key])) {
				var plan = plv8.prepare('select find_conflicts($1, $2, $3)', ['jsonb','jsonb','text']);
				plan.execute({}, obj2[key], currentPath);
            }
        }
    }
$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION has_version_conflict(input_feature JSONB, head_feature JSONB) RETURNS BOOLEAN AS $BODY$
	let inputVersion = -1;
	let headVersion = -1;

	if(input_feature == null || head_feature == null)
		plv8.elog(ERROR, 'Features are not allowed to be null!');

	if(input_feature.properties != undefined && input_feature.properties['@ns:com:here:xyz'] != undefined
		&& input_feature.properties['@ns:com:here:xyz'].version != undefined )
		inputVersion = input_feature.properties['@ns:com:here:xyz'].version;

	if(head_feature.properties != undefined && head_feature.properties['@ns:com:here:xyz'] != undefined
		&& head_feature.properties['@ns:com:here:xyz'].version != undefined )
		headVersion = head_feature.properties['@ns:com:here:xyz'].version;

    //TODO Check if valid
	if(inputVersion == -1 || headVersion == -1)
		return false;
    return headVersion != inputVersion;
$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION enrich_feature(input_feature JSONB, author TEXT, version BIGINT, created_at BIGINT = -1) RETURNS JSONB AS $BODY$
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

	input_feature.properties['@ns:com:here:xyz'].createdAt = (created_at == -1 || created_at == null) ? Date.now() : created_at;
	input_feature.properties['@ns:com:here:xyz'].updatedAt = Date.now();
	input_feature.properties['@ns:com:here:xyz'].version = version;
	input_feature.properties['@ns:com:here:xyz'].author = author;

    return input_feature;
$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION resolveOperation(input_feature JSONB, head_feature JSONB) RETURNS CHAR AS $BODY$
	let deletedFlag;
	let action;

	if(input_feature == null)
		return 'D';

	if(input_feature.properties != undefined && input_feature.properties['@ns:com:here:xyz'] != undefined
		&& input_feature.properties['@ns:com:here:xyz'].deleted != undefined )
		deletedFlag = input_feature.properties['@ns:com:here:xyz'].deleted;

	if(head_feature == null)
        action = 'I';
    else
        action = 'U';

    if(deletedFlag){
        if(action = 'I')
            return 'H';
        else if(action = 'U')
           return 'U';
    }
	return action;
$BODY$ LANGUAGE plv8 IMMUTABLE;
-------------------------------------------------------------------
CREATE OR REPLACE FUNCTION clean_feature(input_feature JSONB) RETURNS JSONB AS $BODY$
	delete input_feature.geometry;
	delete input_feature.properties['@ns:com:here:xyz'].version;
	delete input_feature.properties['@ns:com:here:xyz'].author;

    return input_feature;
$BODY$ LANGUAGE plv8 IMMUTABLE;