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
    const maxBigint = plv8.execute("SELECT max_bigint()")[0].max_bigint[0]

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
        isDelete = false;
        attributeConflicts;

        constructor(inputFeature, version, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial) {
            this.schema = context("schema");
            this.table = context("table");
            this.context = context("context");
            this.historyEnabled = context("historyEnabled");

            this.inputFeature = inputFeature;
            this.version = version;
            this.author = author;
            this.onExists = onExists == null ? 'REPLACE' : onExists;
            this.onNotExists = onNotExists == null ? 'CREATE' : onNotExists;
            this.onVersionConflict = onVersionConflict;
            this.onMergeConflict = onMergeConflict;
            this.isPartial = isPartial;

            this.enrichFeature();
            this.isDelete = this._hasDeletedFlag(this.inputFeature);
        }

        /**
         * @throws VersionConflictError, MergeConflictError, FeatureExistsError
         */
        writeFeature() {
            return this.isDelete ? this.deleteFeature() : this.writeRow();
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
            if (this.onVersionConflict != null) {
                //Version conflict detection is active
                let updatedRows = plv8.execute(`UPDATE "${this.schema}"."${this.table}" SET next_version = $1 WHERE id = $2 AND next_version = $3 AND version = $4 RETURNING *`, this.version, this.inputFeature.id, maxBigint(), this.baseVersion);
                if (updatedRows.length == 1) {
                    if (this.operation == "D") {
                        if (updatedRows[0].operation != "D")
                            this._insertHistoryRow();
                    }
                    else {
                        this.operation = updatedRows[0].operation == "D" ? this.operation : this._transformToUpdate(this.operation);
                        this._insertHistoryRow();
                    }
                }
                else {
                    if (this.loadFeature(this.inputFeature.id) != null)
                        //The feature exists in HEAD and still no previous version was updated, so we have a version conflict
                        this.handleVersionConflict();
                    else {
                        if (updatedRows[0].operation != "D")
                            this._insertHistoryRow();
                    }
                }
            }
            else {
                //Version conflict detection is not active
                let updatedRows = plv8.execute(`UPDATE "${this.schema}"."${this.table}" SET next_version = $1 WHERE id = $2 AND next_version = $3 AND version < $1 RETURNING *`, this.version, this.inputFeature.id, maxBigint());
                if (updatedRows.length == 1)  {
                    if (this.operation == "D") {
                        if (updatedRows[0].operation != "D")
                            this._insertHistoryRow();
                    }
                    else {
                        this.operation = updatedRows[0].operation == "D" ? this.operation : this._transformToUpdate(this.operation);
                        this._insertHistoryRow();
                    }
                }
                else {
                    if (this.operation != "D")
                      this._insertHistoryRow();
                }
            }
        }

        _transformToUpdate(operation) {
            return operation == "I" ? "U" : "J";
        }

        _insertHistoryRow() {
            //TODO: Encode geo as WKB correctly!
            plv8.execute(`INSERT INTO "${this.schema}"."${this.table}"
                (id, version, operation, author, jsondata, geo)
                VALUES (
                    $1, $2, $3, $4,
                    $5::JSONB - 'geometry',
                    CASE WHEN $5->geometry::geometry IS NULL THEN NULL ELSE ST_Force3D(ST_GeomFromWKB($5->geometry::BYTEA, 4326)) END
                )`,
                this.inputFeature.id, this.version, this.operation, this.author, this.inputFeature);
        }

        _operation2HumanReadable(operation) {
            return operation == "I" || operation == "H" ? "insert" : operation == "U" || operation == "J" ? "update" : "delete";
        }

        /**
         * @throws FeatureExistsError
         */
         writeRowWithoutHistory() {
            //TODO: onVersionConflict & baseVersion is missing in method signature
            if (this.onVersionConflict == null) {
                if (this.isPartial)
                    this.inputFeature = this.patch(this.loadFeature(this.inputFeature.id), this.inputFeature);

                    //TODO: Use an ON CONFLICT clause here to directly UPDATE the feature instead of INSERTing it in case of existence (see: simple_upsert)
                    let sql = "INSERT INTO \""+ this.schema + "\".\"" + this.table + "\" AS tbl "
                        + "(id, version, operation, author, jsondata, geo) VALUES ($1, $2, $3, $4, $5, ST_Force3D(ST_GeomFromGeoJSON($6))) ";

                    switch(this.onExists){
                        case "REPLACE" :
                            sql += ` ON CONFLICT (id, version, next_version) DO UPDATE SET
                                  version = greatest(tbl.version, EXCLUDED.version),
                                  operation = 'U',
                                  author = EXCLUDED.author,
                                  jsondata = jsonb_set(EXCLUDED.jsondata, '{properties,@ns:com:here:xyz,createdAt}',
                                         tbl.jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'),
                                  geo = EXCLUDED.geo`;
                            break;
                        case "RETAIN" :
                            sql += " ON CONFLICT(id, version, next_version) DO NOTHING"
                            break;
                        case "DELETE" :
                            return this.deleteFeature();
                        case "ERROR" : break;
                    }

                    sql += " RETURNING COALESCE(jsonb_set(jsondata,'{geometry}',ST_ASGeojson(geo)::JSONB), jsondata) ";

                    //TODO check if there is a possibility without a deep-copy!
                    let featureClone = JSON.parse(JSON.stringify(this.inputFeature));
                    delete featureClone.geometry;

                    let plan = plv8.prepare(sql, ['TEXT','BIGINT','CHAR','TEXT','JSONB','JSONB']);
                    let writtenFeature = plan.execute(this.inputFeature.id,
                        this.version,
                        //TODO set version operation
                        this.operation,
                        this.author,
                        //TODO - check if nessecary - Isnt version & author also written now in status quo impl?
                        featureClone,
                        this.inputFeature.geometry
                    );

                    return writtenFeature.length > 0 ? writtenFeature[0].coalesce : null;
            }else{
                let baseVersion = this.inputFeature.properties[@ns:com:here:xyz].version;

                if(baseVersion == undefined)
                    ;//throw Error!

                let headFeature = this.loadFeature(this.inputFeature.id, baseVersion);
            }
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
            return this.isDelete ? this.handleDeleteVersionConflict() : this.handleWriteVersionConflict();
        }

        handleWriteVersionConflict() {
            switch (this.onVersionConflict) {
                case "MERGE":
                    return this.mergeChanges();
                case "ERROR":
                    this._throwVersionConflictError();
                case "REPLACE": {
                    this.onVersionConflict = null;
                    return this.writeRow();
                }
                case "RETAIN":
                    return null; //TODO: Check status-quo impl whether we should return the existing HEAD instead
            }
        }

        handleDeleteVersionConflict() {
            switch (this.onVersionConflict) {
                case "MERGE":
                    let headFeature = this.loadFeature(this.inputFeature.id);
                    if (!this._hasDeletedFlag(headFeature))
                        //Current HEAD state is not deleted
                        return this.handleMergeConflict();
                    return headFeature;
                case "ERROR":
                    this._throwVersionConflictError();
                case "REPLACE": {
                    this.onVersionConflict = null;
                    return this.deleteRow();
                }
                case "RETAIN":
                    return null; //TODO: Check status-quo impl whether we should return the existing HEAD instead
            }
        }

        _throwVersionConflictError() {
            //TODO: Add error code XYZ49
            plv8.elog(ERROR, `Version conflict while trying to ${this._operation2HumanReadable(this.operation)} feature with ID ${this.inputFeature.id} in version ${this.version}. Base version ${this.baseVersion} is not matching the current HEAD version ${this._getFeatureVersion(this.loadFeature(this.inputFeature.id))}.`);
        }

        /**
         * @throws MergeConflictError
         */
        mergeChanges() {
            //NOTE: This method will *only* be called in case a version conflict was detected and onVersionConflict == MERGE
            let headFeature = this.loadFeature(this.inputFeature.id);
            let baseFeature = this.loadFeature(this.inputFeature.id, this.baseVersion);
            let inputDiff = this.isPartial ? this.inputFeature : this.diff(this.inputFeature, baseFeature); //Our incoming change
            let headDiff = this.diff(headFeature, baseFeature); //The other change
            this.attributeConflicts = this.findConflicts(inputDiff, headDiff);

            if (this.attributeConflicts.length > 0)
                return this.handleMergeConflict();

            this.inputFeature = this.patch(headFeature, inputDiff);
            this.operation = this._transformToUpdate(this.operation);
            this.onVersionConflict = null;
            return this.writeRow();
        }


        //TODO: Harmonize the following two methods
        /**
         * @throws MergeConflictError
         */
        handleMergeConflict() {
            switch (this.onVersionConflict) {
                case "ERROR":
                    this._throwMergeConflictError();
                case "REPLACE": {
                    this.onVersionConflict = null;
                    return this.isDelete ? this.deleteRow() : this.writeRow();
                }
                case "RETAIN":
                    return null; //TODO: Check status-quo impl whether we should return the existing HEAD instead
            }
        }

        _throwMergeConflictError() {
            //TODO: Add error code XYZ49
            //TODO: In case of write, add all conflicting attributes as info (stored in this.attributeConflicts)
            plv8.elog(ERROR, `Merge conflict while trying to ${this._operation2HumanReadable(this.operation)} feature with ID ${this.inputFeature.id} in version ${this.version}. Base version ${this.baseVersion} was not matching the current HEAD version ${this._getFeatureVersion(this.loadFeature(this.inputFeature.id))} and a merge was not possible.`);
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

        _getFeatureVersion(feature) {
            return feature.properties["@ns:com:here:xyz"].version;
        }

        _hasDeletedFlag(feature) {
            return feature.properties["@ns:com:here:xyz"].deleted == true;
        }

        /**
         * Finds all conflicting attributes in two changes / diffs if existing.
         * The returned result is a list of conflicting attributes including the two conflicting values.
         * @private
         * @return {{<string>: [<object>, <object>]}[]}
         */
        findConflicts(diff1, diff2) {
            //TODO: Check if diffs are recursively disjunct or if not being disjunct the according value must be equal
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
            if(target == null)
                return inputDiff;

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
            let feature = this.inputFeature;
            this.baseVersion = feature.properties && feature.properties["@ns:com:here:xyz"] && feature.properties["@ns:com:here:xyz"].version;

            feature.id = feature.id || Math.random().toString(36).slice(2, 10);
            feature.type = feature.type || "Feature";
            this.author = this.author || "ANOYMOUS";
            feature.properties = feature.properties || {};
            feature.properties["@ns:com:here:xyz"] = feature.properties["@ns:com:here:xyz"] || {};

            //TODO: Set the createdAt TS right before writing and only if it is an insert
            let now = Date.now();
            feature.properties["@ns:com:here:xyz"].createdAt = (feature.properties["@ns:com:here:xyz"].createdAt == undefined) ? now : (feature.properties["@ns:com:here:xyz"].createdAt);
            feature.properties["@ns:com:here:xyz"].updatedAt = now;
            feature.properties["@ns:com:here:xyz"].version = this.version;
            feature.properties["@ns:com:here:xyz"].author = this.author;
        }
    }
    plv8.FeatureWriter = FeatureWriter;
    //Init completed


    //Run the actual command
    return new FeatureWriter(input_feature, version, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial).writeFeature();
$BODY$ LANGUAGE plv8 IMMUTABLE;