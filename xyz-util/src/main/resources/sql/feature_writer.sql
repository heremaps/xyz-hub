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
    const maxBigint = plv8.execute("SELECT max_bigint()")[0].max_bigint[0];

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
    return plv8.execute("SELECT nextval($1)", fullQualifiedSequenceName)[0].nextval;
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


    //TODO: Move classes to own JS file that can then be included by Script installer
    class Exception extends Error {
      code = 0;
      context;
      detail;
      hint;

      constructor(message) {
        super(message);
        this.withDetail(this.constructor.name + ": ");
      }

      withCode(code) {
        //Valid characters are within the range of chars: "0" (ASCII: 49) - "o" (ASCII: 111), 00000 however will be mapped to "XX000"
        if (code == null || typeof code != "string" || code.length != 5)
          return this;

        let numericCode = 0;
        for (let i in code)
          numericCode += Math.pow(64, i) * (code[i].charCodeAt(0) - 48);
        this.code = numericCode;
        return this;
      }

      withContext(context) {
        this.context = context;
        return this;
      }

      withDetail(detail) {
        this.detail = detail;
        return this;
      }

      withHint(hint) {
        this.hint = hint;
        return this;
      }
    }

    class XyzException extends Exception {
      constructor(message) {
        super(message);
        this.withCode("XYZ50");
      }
    }

    class VersionConflictError extends XyzException {
      constructor(message) {
        super(message);
        this.withCode("XYZ49");
      }
    }

    class MergeConflictError extends VersionConflictError {
      constructor(message) {
        super(message);
        this.withCode("XYZ48");
      }
    }

    class IllegalArgumentException extends XyzException {
      constructor(message) {
        super(message);
        this.withCode("XYZ40");
      }
    }

    class FeatureExistsException extends XyzException {
      constructor(message) {
        super(message);
        this.withCode("XYZ44");
      }
    }

    class FeatureNotExistsException extends XyzException {
      constructor(message) {
        super(message);
        this.withCode("XYZ45");
      }
    }

    /**
     * The unified implementation of the database-based feature writer.
     */
    class FeatureWriter {
        maxBigint = 9223372036854775807;

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
        ignoreConflictPaths = {
          "properties.@ns:com:here:xyz.author": true,
          "properties.@ns:com:here:xyz.version": true,
          "properties.@ns:com:here:xyz.createdAt": true,
          "properties.@ns:com:here:xyz.updatedAt": true
        };

        constructor(inputFeature, version, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial) {
            if (isPartial && onNotExists != null)
                throw new IllegalArgumentException("onNotExists must not be defined for partial writes.");

            this.schema = context("schema");
            this.table = context("table");
            this.context = context("context");
            this.historyEnabled = context("historyEnabled");

            this.inputFeature = inputFeature;
            this.isDelete = this._hasDeletedFlag(this.inputFeature);
            this.version = version;
            this.author = author;
            this.onExists = onExists || "REPLACE";
            this.onNotExists = onNotExists || (isPartial ? "ERROR" : "CREATE");
            this.onVersionConflict = onVersionConflict == null ? null : onVersionConflict || (this.isDelete ? "REPLACE" : "MERGE");
            this.onMergeConflict = onMergeConflict == null ? "ERROR" : onMergeConflict;
            this.isPartial = isPartial;

            this.enrichFeature();
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
                let updatedRows = plv8.execute(`UPDATE "${this.schema}"."${this.table}" SET next_version = $1 WHERE id = $2 AND next_version = $3 AND version = $4 RETURNING *`, this.version, this.inputFeature.id, this.maxBigint, this.baseVersion);
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
                switch(this.onExists){
                    case "RETAIN":
                        this.headFeature = this.loadFeature(this.inputFeature.id);
                        if(this.headFeature != null)
                            return null;
                    case "ERROR": {
                        this.headFeature = this.loadFeature(this.inputFeature.id);
                        if(this.headFeature != null)
                            this._throwFeatureExistsError();
                    }
                }
                //Version conflict detection is not active
                let updatedRows = plv8.execute(`UPDATE "${this.schema}"."${this.table}" SET next_version = $1 WHERE id = $2 AND next_version = $3 AND version < $1 RETURNING *`, this.version, this.inputFeature.id, this.maxBigint);
                if (updatedRows.length == 1)  {
                    switch(this.onExists){
                        case "DELETE" : this.operation = "D"; break;
                    }
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
                    switch(this.onNotExists){
                        case "CREATE" : break; //NOTHING TO DO;
                        case "ERROR":
                            this._throwFeatureNotExistsError();
                        case "RETAIN" :
                            return null;
                    }

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
                    ST_Force3D(ST_GeomFromGeoJSON($6::JSONB))
                )`,
                this.inputFeature.id, this.version, this.operation, this.author, this.inputFeature,  this.inputFeature.geometry);
        }

        _operation2HumanReadable(operation) {
            return operation == "I" || operation == "H" ? "insert" : operation == "U" || operation == "J" ? "update" : "delete";
        }

        /**
         * @throws FeatureExistsError
         */
         writeRowWithoutHistory() {
              if (this.isPartial)
                this.inputFeature = this.patch(this.loadFeature(this.inputFeature.id), this.inputFeature);

            if (this.onVersionConflict == null) {

                //TODO: Use an ON CONFLICT clause here to directly UPDATE the feature instead of INSERTing it in case of existence (see: simple_upsert)
                let sql = `INSERT INTO "${this.schema}"."${this.table}" AS tbl
                            (id, version, operation, author, jsondata, geo) VALUES ($1, $2, $3, $4, $5 - 'geometry', ST_Force3D(ST_GeomFromGeoJSON($6)))`;

                switch(this.onExists){
                    case "REPLACE" :
                        sql += `ON CONFLICT (id, next_version) DO UPDATE SET
                              version = greatest(tbl.version, EXCLUDED.version),
                              operation = 'U',
                              author = EXCLUDED.author,
                              jsondata = jsonb_set(EXCLUDED.jsondata, '{properties,@ns:com:here:xyz,createdAt}',
                                     tbl.jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'),
                              geo = EXCLUDED.geo `;
                        break;
                    case "RETAIN" :
                        sql += " ON CONFLICT(id, version, next_version) DO NOTHING "
                        break;
                    case "DELETE" :
                        return this.deleteFeature();
                    case "ERROR" : break;
                        //TODO Catch exception and thrown own exception?
                }

                //sql += "RETURNING COALESCE(jsonb_set(jsondata,'{geometry}',ST_ASGeojson(geo)::JSONB) as feature) ";
                sql += "RETURNING (jsondata->'properties'->'@ns:com:here:xyz'->'createdAt') as created_at, operation ";

                //TODO check if there is a possibility without a deep-copy!
                let featureClone = JSON.parse(JSON.stringify(this.inputFeature));
                delete featureClone.geometry;

                let plan = plv8.prepare(sql, ['TEXT','BIGINT','CHAR','TEXT','JSONB','JSONB']);

                try{
                   let writtenFeature = plan.execute(this.inputFeature.id,
                        this.version,
                        //TODO set version operation
                        this.operation,
                        this.author,
                        this.inputFeature,
                        this.inputFeature.geometry
                    );

                    if(writtenFeature.length == 0){
                      if(e.sqlerrcode == '23505' && this.onExists != 'RETAIN')
                        this._throwFeatureExistsError();

                      throw new XyzException("Unexpected Error!")
                        .withDetail(e.detail)
                        .withHint(e.hint);
                    }

                    if(writtenFeature[0].operation == 'U'){
                        //Inject createdAt
                        this.inputFeature.properties['@ns:com:here:xyz'].createdAt = writtenFeature[0].created_at[0];
                    }else{
                       switch(this.onNotExists){
                            case "CREATE" : break; //NOTHING TO DO;
                            case "ERROR":
                                this._throwFeatureNotExistsError();
                            case "RETAIN" :
                                //TODO solve upsert
                                this.deleteFeature();
                            return null;
                        }
                    }
                }catch(e){
                    if(e.sqlerrcode != undefined && e.sqlerrcode == '23505'){
						if(this.onExists == 'RETAIN')
						    return null;
                        this._throwFeatureExistsError();
                    }
                    //TODO - which kind of error we want to use here?
                    throw e;
                }
                return this.inputFeature;
            }else{
                plv8.elog(NOTICE, "version conflict handling! Base version:",this.baseVersion);

                if (this.baseVersion == undefined)
                    throw new IllegalArgumentException("Provided Feature does not have a baseVersion!");

	            /**
	                TODO : implement non-conflict Case! In this case we
					Check if we need onExists handling here. If its present is has a higher priority as onVersionConflict.
	            */

				if(this.onExists == 'DELETE' || this.onExists == 'RETAIN' || this.onExists == 'ERROR'
					|| this.onNotExists == 'RETAIN' || this.onNotExists == 'ERROR'){
					this.headFeature = this.loadFeature(this.inputFeature.id);
				}

				switch(this.onExists){
                    case "DELETE" :
						if(this.headFeature != null){
							return this.deleteFeature();
                        }
                        break;
                    case "RETAIN" :
                        if(this.headFeature != null)
                            return null;
                        break;
                    case "ERROR" :
                        if(this.headFeature != null)
                            this._throwFeatureExistsError();
                    //NOT handling REPLACE
                }

                switch(this.onNotExists){
                    case "RETAIN" :
                        if(this.headFeature == null)
                            return null;
                        break;
                    case "ERROR" :
                        if(this.headFeature == null)
                            this._throwFeatureNotExistsError();
                    //NOT handling CREATE
                }

                let featureClone = JSON.parse(JSON.stringify(this.inputFeature));
                delete featureClone.geometry;

                let sql = `UPDATE "${this.schema}"."${this.table}" AS tbl
                          SET version = $1,
                          operation = $2,
                          author = $3,
                          jsondata = jsonb_set($4, '{properties,@ns:com:here:xyz,createdAt}',
                                    tbl.jsondata->'properties'->'@ns:com:here:xyz'->'createdAt'),
                          geo = ST_Force3D(ST_GeomFromGeoJSON($5))
                        WHERE
                            id = $6 AND version = $7
                        RETURNING (jsondata->'properties'->'@ns:com:here:xyz'->'createdAt') as created_at, operation `;

                let plan = plv8.prepare(sql, ['BIGINT','CHAR','TEXT','JSONB','JSONB','TEXT','BIGINT']);
                let writtenFeature = plan.execute(
                    this.version,
                    //TODO set version operation
                    'U',
                    this.author,
                    featureClone,
                    this.inputFeature.geometry,
                    this.inputFeature.id,
                    this.baseVersion
                );

                if(writtenFeature.length == 0){
                    //plv8.elog(NOTICE, "Version conflict");
                    return this.handleVersionConflict()
                }
            }
        }

        _throwFeatureExistsError() {
            throw new FeatureExistsException(`Feature with ID ${this.inputFeature.id} exists!`).withCode("XYZ44");
        }

        _throwFeatureNotExistsError() {
            throw new FeatureNotExistsException(`Feature with ID ${this.inputFeature.id} not exists!`).withCode("XYZ45");
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
            let sql = "DELETE FROM \""+ this.schema + "\".\"" + this.table + "\" WHERE id = $1 ";
            let plan = plv8.prepare(sql, ["TEXT"]);

            if (this.onVersionConflict == null)
                //TODO: Use plv8.execute() directly!
                cnt = plv8.prepare(sql, ["TEXT"]).execute(this.inputFeature.id);
            else {
                if (this.baseVersion == 0) {
                  sql += 'AND next_version = max_bigint();'
                  cnt = plan.execute(this.inputFeature.id);
                }
                else {
                  //TODO: Use plv8.execute() directly!
                  sql += 'AND version = $2;'
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

        /**
         * @throws VersionConflictError
         */
        handleWriteVersionConflict() {
            switch (this.onVersionConflict) {
                case "MERGE":
                    if(!this.historyEnabled)
                        throw new IllegalArgumentException("MERGE is not allowed for spaces without history!");
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

        /**
         * @throws VersionConflictError, MergeConflictError
         */
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
            throw new VersionConflictError(`Version conflict while trying to ${this._operation2HumanReadable(this.operation)} feature with ID ${this.inputFeature.id} in version ${this.version}.`)
                .withHint(`Base version ${this.baseVersion} is not matching the current HEAD version ${this._getFeatureVersion(this.loadFeature(this.inputFeature.id))}.`)
        }

        /**
         * @throws MergeConflictError
         */
        mergeChanges() {
            plv8.elog(NOTICE, "MERGE changes",this.onVersionConflict, this.onMergeConflict ); //TODO: Use debug logging
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
            switch (this.onMergeConflict) {
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
            let error = new MergeConflictError(`Merge conflict while trying to ${this._operation2HumanReadable(this.operation)} feature with ID ${this.inputFeature.id} in version ${this.version}.`)
                .withHint(`Base version ${this.baseVersion} was not matching the current HEAD version ${this._getFeatureVersion(this.loadFeature(this.inputFeature.id))} and a merge was not possible.`);

            if (!this.isDelete) {
                let detail = `The following conflicts occurred for the feature with ID ${this.inputFeature.id}:`;
                for (let conflict of this.attributeConflicts) {
                    let path = Object.getOwnPropertyNames(conflict)[0];
                    detail += "\n\t" + `- ${path}: ${typeof conflict[path][0] == "string" ? `"${conflict[path][0]}"` : conflict[path][0]} <> ${typeof conflict[path][1] == "string" ? `"${conflict[path][1]}"` : conflict[path][1]}`
                }
                error.withDetail(detail);
            }

            throw error;
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
                //Unexpected exception:
                throw new XyzException("Found two Features with the same id!");
        }

        _getFeatureVersion(feature) {
            return feature.properties["@ns:com:here:xyz"].version;
        }

        _hasDeletedFlag(feature) {
            return this.inputFeature.properties == undefined
				|| this.inputFeature.properties["@ns:com:here:xyz"] == undefined ?
			false : this.inputFeature.properties["@ns:com:here:xyz"].deleted == true;
        }

        //Helper function to determine if a value is an object
        _isObject(obj) {
            return obj != null && typeof obj == "object" && !Array.isArray(obj);
        }

        /**
         * Finds all conflicting attributes in two changes / diffs if existing.
         * The returned result is a list of conflicting attributes including the two conflicting values.
         * @private
         * @return {{<string>: [<object>, <object>]}[]}
         */
        findConflicts(obj1, obj2, path = "") {
            let conflicts = [];

            //Check if diffs are recursively disjunct or if not being disjunct. The according value must be equal.
            //Iterate over keys of the first object
            for (const key in obj1) {
                if (key in obj1) {
                    const currentPath = path ? path + "." + key : key;
                    if (key in obj2) {
                        //If both values are objects => recursion
                        if (this._isObject(obj1[key]) && this._isObject(obj2[key]))
                            conflicts = conflicts.concat(this.findConflicts(obj1[key], obj2[key], currentPath));
                        else if (obj1[key] !== obj2[key]) {
                            //Fix 2d coords => 3d coords
                            if (currentPath == "geometry.coordinates")
                                this._ensure3d(obj1[key]);
                            else if (this.ignoreConflictPaths[currentPath])
								continue;

							if (Array.isArray(obj1[key]) && Array.isArray(obj2[key])
                                //TODO: Improve performance of the following
								&& JSON.stringify(obj1[key]) == JSON.stringify(obj2[key]))
								continue;

                            conflicts.push({[currentPath]: [obj1[key], obj2[key]]});
                        }
                    }
                }
            }

            //Iterate over keys of the second object
            for (const key in obj2) {
                if (key in obj2 && !(key in obj1)) {
                    const currentPath = path ? path + "." + key : key;

                    if (this._isObject(obj2[key]) && !this._isObject(obj1[key]))
                        conflicts = conflicts.concat(this.findConflicts({}, obj2[key], currentPath));
                }
            }

            return conflicts;
        }

        _ensure3d(geometry) {
          //FIXME: That only works for points, but not for lines (=> check type and act accordingly)
          if (geometry.length == 2)
            geometry.push(0);
        }

        diff(minuend, subtrahend) {
            plv8.elog(NOTICE, "diff ", JSON.stringify(minuend), " ", JSON.stringify(subtrahend)); //TODO: Use debug logging
            let diff = {};

            if (minuend == null)
				return subtrahend;
			if (subtrahend == null)
				return minuend;

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

        /**
         * NOTE: target is mandatory to be a valid (existing) feature
         */
        patch(target, inputDiff) {
            //plv8.elog(NOTICE, "patch ", JSON.stringify(target), " ", JSON.stringify(inputDiff)); //TODO: Use debug logging
            for (let key in inputDiff) {
                if (inputDiff.hasOwnProperty(key)) {
                    if (inputDiff[key] === null)
                        delete target[key];
                    else if (typeof inputDiff[key] == "object" && !Array.isArray(inputDiff[key]) && inputDiff[key] !== null) {
                        if (!target[key])
                          target[key] = {};
                        //TODO: Deactivate notices depending on a DEBUG env variable
                        //plv8.elog(NOTICE, "patch [", key, "] -> ", JSON.stringify(target[key]), "-", JSON.stringify(inputDiff[key]));
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