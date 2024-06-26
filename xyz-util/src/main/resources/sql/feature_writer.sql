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

CREATE EXTENSION IF NOT EXISTS plv8;

/**
 * @public
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_features(input_features TEXT, author TEXT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN)
    RETURNS JSONB AS $BODY$

    //Import other functions
    const writeFeature = plv8.find_function("write_feature");
    const getNextVersion = plv8.find_function("get_next_version");
    const context = plv8.context = key => plv8.execute("SELECT context($1)", key)[0].context[0];

    //Actual executions
    if (input_features == null)
        plv8.elog(ERROR, "Parameter input_features must not be null.");

    //TODO: Compare JSON parsing performance of PLSQL vs PLV8
    let features = JSON.parse(input_features);
    let version = getNextVersion();

    for (let feature of features)
        writeFeature(feature, version, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial);
$BODY$ LANGUAGE plv8 IMMUTABLE;


/**
 * @private
 */
CREATE OR REPLACE FUNCTION get_next_version() RETURNS BIGINT AS $BODY$
    //Import other functions
    const context = plv8.context;

    const VERSION_SEQUENCE_SUFFIX = "_version_seq";
    let sequenceName = context("table") + VERSION_SEQUENCE_SUFFIX;
    let fullQualifiedSequenceName = "\"" + context("schema") + "\".\"" + sequenceName + "\"";

    //Actual executions
    plv8.execute("SELECT nextval($1)", fullQualifiedSequenceName)[0].nextval[0];
$BODY$ LANGUAGE plv8 IMMUTABLE;


/**
 * @public
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_feature(input_feature JSONB, version BIGINT, author TEXT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN)
    RETURNS JSONB AS $BODY$

    //Init block of internal feature_writer functionality
    const context = plv8.context = key => plv8.execute("SELECT context($1)", key)[0].context[0];

    /**
     * The unified implementation of the database-based feature writer.
     */
    class FeatureWriter {
        //Process input fields
        inputFeature;
        version;
        author;
        isPartial;
        baseVersion;

        onExists;
        onNotExists;
        onVersionConflict;
        onMergeConflict;

        //Process generated / tmp fields
        headFeature;
        operation = "I";

        constructor(inputFeature, version, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial) {
            this.schema = context("schema");
            this.table = context("table");
            this.context = context("context");
            this.historyEnabled = context("historyEnabled");

            this.inputFeature = inputFeature;
            this.version = version;
            this.author = author;
            this.onExists = onExists;
            this.onNotExists = onNotExists;
            this.onVersionConflict = onVersionConflict;
            this.onMergeConflict = onMergeConflict;
            this.isPartial = isPartial;
            this.enrichFeature();
        }

        /**
         * @throws VersionConflictError, MergeConflictError, FeatureExistsError
         */
        writeFeature() {
            if (this.inputFeature.properties["@ns:com:here:xyz"].deleted == true)
                this.deleteFeature();

            return this.writeRow();
        }

        /**
         * @throws VersionConflictError, MergeConflictError
         */
        deleteFeature() {
            if (this.context == "DEFAULT") {
                //TODO:
            }
            else {
                //NULL - EXTENSION
                if (this.historyEnabled) {
                    this.onExists = "DELETE";
                    this.onNotExists = "RETAIN";
                    return this.writeRow();
                }
                return this.deleteRow();
          }
        }

        /**
         * @throws VersionConflictError, MergeConflictError, FeatureExistsError
         */
        writeRow() {
          if (this.historyEnabled)
            return this.writeRowWithHistory();
          return this.writeRowWithoutHistory();
        }

        /**
         * @throws VersionConflictError, MergeConflictError, FeatureExistsError
         */
        writeRowWithHistory() {

        }

        /**
         * @throws FeatureExistsError
         */
        writeRowWithoutHistory() {
            //TODO: onVersionConflict & baseVersion is missing in method signature
            if (this.onVersionConflict == null) {
                let feature = this.inputFeature;
                if (this.isPartial)
                  //TODO: inputFeature has to be enriched already!
                  feature = this.patch(this.loadFeature(inputFeature.id), inputFeature);

                //TODO: Use an ON CONFLICT clause here to directly UPDATE the feature instead of INSERTing it in case of existence (see: simple_upsert)
                let sql = "INSERT INTO \""+ this.schema + "\".\"" + this.table + "\""
                    + "(id, version, operation, author, jsondata, geo) VALUES ($1, $2, $3, $4, $5, ST_Force3D(ST_GeomFromGeoJSON($6)))";

                //TODO: Use plv8.execute() directly!
                let plan = plv8.prepare(sql, ['TEXT','BIGINT','CHAR','TEXT','JSONB','JSONB']);
                try {
                  let cnt = plan.execute(feature.id,
                      feature.properties["@ns:com:here:xyz"].version,
                      //TODO set version operation
                      this.operation,
                      feature.properties["@ns:com:here:xyz"].author,
                      //TODO - check if nessecary - Isnt version & author also written now in status quo impl?
                      plv8.find_function("clean_feature(jsonb)")(jsondata),
                      feature.geometry);
                }
                catch(e) {
                  //UNIQUE VIOLATION
                  if (e.sqlerrcode != undefined && e.sqlerrcode == '23505')
                      this.handleVersionConflict()
                  else
                      plv8.elog(ERROR, e);
                }
            }
            plan.free();
        }

        /**
         * @throws VersionConflictError
         */
        deleteRow() {
            //TODO: do we need the payload of the feature as return?
            let cnt;
            //base_version get provided from user (extend api endpoint if necessary)
            //TODO: Deactivate notices depending on a DEBUG env variable
            plv8.elog(NOTICE, "Delete id = ", this.inputFeature.id);
            let sql = "DELETE FROM \""+ this.schema + "\".\"" + this.table + "\" WHERE id = $1";
            let plan = plv8.prepare(sql, ["TEXT"]);

            if (this.onVersionConflict == null)
                //TODO: Use plv8.execute() directly!
                cnt = plv8.prepare(sql, ["TEXT"]).execute(this.inputFeature.id);
            else {
                if (base_version == 0) {
                  sql = 'AND next_version = max_bigint();'
                  cnt = plan.execute(this.inputFeature.id);
                }
                else {
                  //TODO: Use plv8.execute() directly!
                  sql = 'AND version = $2;'
                  plan = plv8.prepare(sql, ["TEXT", "BIGINT"]);
                  cnt = plan.execute(this.inputFeature.id, this.baseVersion);
                }

                if (cnt == 0) {
                  plv8.elog(NOTICE, "HandleConflict for id=", this.inputFeature.id,);
                  //handleDeleteVersionConflict
                }
            }
            plan.free();
        }

        /**
         * @throws VersionConflictError, MergeConflictError
         */
        handleVersionConflict() {

        }

        /**
         * @throws MergeConflictError
         */
        mergeChanges() {

        }

        /**
         * @throws MergeConflictError
         */
        handleMergeConflict() {

        }

        loadFeature(id, version = "HEAD") {
            if (version == "HEAD" && this.headFeature)
              return this.headFeature;

            //TODO: Check if we need a check on operation.
            if (id == null)
                return null;

            //TODO: Use template strings instead!
            let sql = "SELECT id, version, author, jsondata, geo::JSONB "  //operation,next_version,i
              + "FROM \""+ this.schema + "\".\"" + this.table + "\"";

            let res;
            if (version == "HEAD") {
                //next_version + operation supports head retrival if we have multiple versions
                sql += "WHERE id = $1 "
                    + "AND next_version = max_bigint() "
                    + "AND operation != $2";
                //TODO: Use plv8.execute() directly!
                let plan = plv8.prepare(sql, ["TEXT","CHAR"]);
                res = plan.execute(id, "D");
                plan.free();
            }
            else {
                sql += "WHERE id = $1 "
                    + "AND version = $2 "
                    + "AND operation != $3";
                //TODO: Use plv8.execute() directly!
                let plan = plv8.prepare(sql, ["TEXT", "BIGINT", "CHAR"]);
                res = plan.execute(id, version, "D");
                plan.free();
            }

            if (res.length == 0)
                return null;
            else if (res.length == 1) {
                let feature = res[0].jsondata;
                feature.id = res[0].id;
                feature.geometry = res[0].geo;
                feature.properties["@ns:com:here:xyz"].version = res[0].version;
                feature.properties["@ns:com:here:xyz"].author = res[0].author;
                if (feature.geometry != null)
                  delete feature.geometry.crs; //TODO: What is crs?!
                //Cache the HEAD-feature in case its needed later again
                if (version == "HEAD")
                    this.headFeature = feature;
                return feature;
            }
            else
                plv8.elog(ERROR, "Found two Features with the same id!");
        }

        diff(minuend, subtrahend) {
            let diff = {};

            //TODO: Ensure that null values are treated correctly!
            for (let key in subtrahend) {
                if (subtrahend.hasOwnProperty(key)) {
                    if (typeof subtrahend[key] == "object" && !Array.isArray(subtrahend[key]) && subtrahend[key] !== null) {
                        //Recursively diff nested objects
                        let nestedDiff = this.diff(minuend[key] || {}, subtrahend[key]);

                        if (Object.keys(nestedDiff).length > 0)
                          diff[key] = nestedDiff;
                    }
                    else if (minuend[key] !== subtrahend[key])
                        //Add changed or new properties
                        diff[key] = subtrahend[key];
                }
            }

            // Check for removed properties
            for (let key in minuend)
                if (minuend.hasOwnProperty(key) && !subtrahend.hasOwnProperty(key))
                  diff[key] = null;

            return diff;
        }

        patch(target, inputDiff) {
            for (let key in inputDiff) {
                if (inputDiff.hasOwnProperty(key)) {
                    if (inputDiff[key] === null)
                        delete target[key];
                    else if (typeof inputDiff[key] === 'object' && !Array.isArray(inputDiff[key]) && inputDiff[key] !== null) {
                        if (!target[key])
                          target[key] = {};
                        //TODO: Deactivate notices depending on a DEBUG env variable
                        plv8.elog(NOTICE, 'patch [', key, '] -> ', JSON.stringify(target[key]), '-', JSON.stringify(inputDiff[key]));
                        target[key] = this.patch(target[key], inputDiff[key]);
                    }
                    else
                        target[key] = inputDiff[key];
                }
            }
            return target;
        }

        enrichFeature() {
            this.baseVersion = this.inputFeature.properties && this.inputFeature.properties["@ns:com:here:xyz"] && this.inputFeature.properties["@ns:com:here:xyz"].version;

            this.inputFeature.id = this.inputFeature.id || Math.random().toString(36).slice(2, 10);
            this.inputFeature.type = this.inputFeature.type || "Feature";
            this.author = this.author || "ANOYMOUS";
            this.inputFeature.properties = this.inputFeature.properties || {};
            this.inputFeature.properties["@ns:com:here:xyz"] = this.inputFeature.properties["@ns:com:here:xyz"] || {};

            //TODO: Set the createdAt TS right before writing and only if it is an insert
            let now = Date.now();
            this.inputFeature.properties["@ns:com:here:xyz"].createdAt = (this.inputFeature.properties["@ns:com:here:xyz"].createdAt == undefined) ? now : (input_feature.properties["@ns:com:here:xyz"].createdAt);
            this.inputFeature.properties["@ns:com:here:xyz"].updatedAt = now;
            this.inputFeature.properties["@ns:com:here:xyz"].version = this.version;
            this.inputFeature.properties["@ns:com:here:xyz"].author = this.author;
        }
    }
    plv8.FeatureWriter = FeatureWriter;
    //Init completed


    //Run the actual command
    return new FeatureWriter(input_feature, version, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial).writeFeature();
$BODY$ LANGUAGE plv8 IMMUTABLE;