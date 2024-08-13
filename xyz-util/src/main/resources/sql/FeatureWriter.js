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
 * The unified implementation of the database-based feature writer.
 */
class FeatureWriter {
  maxBigint = "9223372036854775807"; //NOTE: Must be a string because of JS precision
  XYZ_NS = "@ns:com:here:xyz";

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
    [`properties.${this.XYZ_NS}.author`]: true,
    [`properties.${this.XYZ_NS}.version`]: true,
    [`properties.${this.XYZ_NS}.createdAt`]: true,
    [`properties.${this.XYZ_NS}.updatedAt`]: true
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

    if (this.onVersionConflict == "MERGE" && !this.historyEnabled)
      throw new IllegalArgumentException("MERGE can not be executed for spaces without history!");

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
    switch (this.onExists) {
      case "RETAIN":
        this.headFeature = this.loadFeature(this.inputFeature.id);
        if (this.headFeature != null)
          return null;
      case "ERROR": {
        this.headFeature = this.loadFeature(this.inputFeature.id);
        if (this.headFeature != null)
          this._throwFeatureExistsError();
      }
    }

    if (this.onVersionConflict != null) {
      //Version conflict detection is active
      let updatedRows = plv8.execute(`UPDATE "${this.schema}"."${this.table}"
                                      SET next_version = $1
                                      WHERE id = $2
                                        AND next_version = $3::BIGINT
                                        AND version = $4
                                      RETURNING *`, this.version, this.inputFeature.id, this.maxBigint, this.baseVersion);
      if (updatedRows.length == 1) {
        if (this.onExists == "DELETE")
          this.operation = "D";

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
      let updatedRows = plv8.execute(`UPDATE "${this.schema}"."${this.table}"
                                      SET next_version = $1
                                      WHERE id = $2
                                        AND next_version = $3::BIGINT
                                        AND version < $1
                                      RETURNING *`, this.version, this.inputFeature.id, this.maxBigint);
      if (updatedRows.length == 1) {
        if (this.onExists == "DELETE")
          this.operation = "D";

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
        switch (this.onNotExists) {
          case "CREATE" :
            break; //NOTHING TO DO;
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
    return operation == "I" || operation == "U" ? "U" : "J";
  }

  _insertHistoryRow() {
    //TODO: Encode geo as WKB correctly!
    plv8.execute(`INSERT INTO "${this.schema}"."${this.table}"
                      (id, version, operation, author, jsondata, geo)
                  VALUES ($1, $2, $3, $4,
                          $5::JSONB - 'geometry',
                          CASE
                              WHEN ($5::JSONB)->'geometry' IS NULL THEN NULL
                              ELSE ST_Force3D(ST_GeomFromGeoJSON(($5::JSONB)->'geometry')) END)`,
        this.inputFeature.id, this.version, this.operation, this.author, this.inputFeature);
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
                     (id, version, operation, author, jsondata, geo)
                 VALUES ($1, $2, $3, $4, $5::JSONB - 'geometry', ST_Force3D(ST_GeomFromGeoJSON($6::JSONB)))`;

      switch (this.onExists) {
        case "REPLACE" :
          sql += ` ON CONFLICT (id, next_version) DO UPDATE SET
                              version = greatest(tbl.version, EXCLUDED.version),
                              operation = 'U',
                              author = EXCLUDED.author,
                              jsondata = jsonb_set(EXCLUDED.jsondata, '{properties, ${this.XYZ_NS}, createdAt}',
                                     tbl.jsondata->'properties'->'${this.XYZ_NS}'->'createdAt'),
                              geo = EXCLUDED.geo`;
          break;
        case "RETAIN" :
          sql += " ON CONFLICT(id, version, next_version) DO NOTHING"
          break;
        case "DELETE" :
          return this.deleteFeature();
        case "ERROR" :
          break;
          //TODO Catch exception and thrown own exception?
      }

      //sql += " RETURNING COALESCE(jsonb_set(jsondata,'{geometry}',ST_ASGeojson(geo)::JSONB) as feature)";

      try {
        let writtenFeature = plv8.execute(sql + ` RETURNING (jsondata->'properties'->'${this.XYZ_NS}'->'createdAt') as created_at, operation `,
            this.inputFeature.id,
            this.version,
            this.operation, //TODO set version operation
            this.author,
            this.inputFeature,
            this.inputFeature.geometry
        );

        if (writtenFeature.length == 0) {
          if (e.sqlerrcode == "23505" && this.onExists != "RETAIN")
            this._throwFeatureExistsError();

          throw new XyzException("Unexpected Error!")
          .withDetail(e.detail)
          .withHint(e.hint);
        }

        if (writtenFeature[0].operation == "U")
            //Inject createdAt
          this.inputFeature.properties[this.XYZ_NS].createdAt = writtenFeature[0].created_at[0];
        else {
          switch (this.onNotExists) {
            case "CREATE" :
              break; //NOTHING TO DO;
            case "ERROR":
              this._throwFeatureNotExistsError();
            case "RETAIN" :
              //TODO solve upsert
              this.deleteFeature();
              return null;
          }
        }
      }
      catch (e) {
        if (e.sqlerrcode == "23505") {
          if (this.onExists == "RETAIN")
            return null;
          this._throwFeatureExistsError();
        }
        //TODO - which kind of error we want to use here?
        throw e;
      }
      return this.inputFeature;
    }
    else {
      plv8.elog(NOTICE, "version conflict handling! Base version:", this.baseVersion);

      if (this.baseVersion == undefined)
        throw new IllegalArgumentException("Provided Feature does not have a baseVersion!");

      /**
       TODO : implement non-conflict Case! In this case we
       Check if we need onExists handling here. If its present is has a higher priority as onVersionConflict.
       */

      let headFeature;

      if (this.onExists == "DELETE" || this.onExists == "RETAIN"
          || this.onExists == "ERROR" || this.onNotExists == "RETAIN"
          || this.onNotExists == "ERROR")
        headFeature = this.loadFeature(this.inputFeature.id);

      switch (this.onExists) {
        case "DELETE" :
          if (headFeature != null)
            return this.deleteFeature();
          break;
        case "RETAIN" :
          if (headFeature != null)
            return null;
          break;
        case "ERROR" :
          if (headFeature != null)
            this._throwFeatureExistsError();
          //NOT handling REPLACE
      }

      switch (this.onNotExists) {
        case "RETAIN" :
          if (headFeature == null)
            return null;
          break;
        case "ERROR" :
          if (headFeature == null)
            this._throwFeatureNotExistsError();
          //NOT handling CREATE
      }

      let featureClone = JSON.parse(JSON.stringify(this.inputFeature));
      delete featureClone.geometry;

      let writtenFeature = plv8.execute(`UPDATE "${this.schema}"."${this.table}" AS tbl
                                         SET version   = $1,
                                             operation = $2,
                                             author    = $3,
                                             jsondata  = jsonb_set($4::JSONB, '{properties, ${this.XYZ_NS}, createdAt}',
                                                                   tbl.jsondata->'properties'->'${this.XYZ_NS}'->'createdAt'),
                                             geo       = ST_Force3D(ST_GeomFromGeoJSON($5::JSONB))
                                         WHERE id = $6
                                           AND version = $7
                                         RETURNING (jsondata->'properties'->'${this.XYZ_NS}'->'createdAt') as created_at, operation`,
          this.version, "U" /*TODO set version operation*/, this.author, featureClone, this.inputFeature.geometry, this.inputFeature.id,
          this.baseVersion);

      if (writtenFeature.length == 0)
        return this.handleVersionConflict()
    }
  }

  _throwFeatureExistsError() {
    throw new FeatureExistsException(`Feature with ID ${this.inputFeature.id} exists!`);
  }

  _throwFeatureNotExistsError() {
    throw new FeatureNotExistsException(`Feature with ID ${this.inputFeature.id} not exists!`);
  }

  /**
   * @throws VersionConflictError
   */
  deleteRow() {
    //TODO: do we need the payload of the feature as return?
    let cnt;
    //base_version get provided from user (extend api endpoint if necessary)
    //TODO: Deactivate notices depending on a DEBUG env variable
    plv8.elog(NOTICE, "Delete feature with id ", this.inputFeature.id);
    let sql = `DELETE FROM "${this.schema}"."${this.table}" WHERE id = $1 `;

    if (this.onVersionConflict == null)
      cnt = plv8.execute(sql, this.inputFeature.id);
    else {
      if (this.baseVersion == 0)
        cnt = plv8.execute(sql + "AND next_version = max_bigint();", this.inputFeature.id);
      else
        cnt = plv8.execute(sql + "AND version = $2;", this.inputFeature.id, this.baseVersion);

      if (cnt == 0) {
        plv8.elog(NOTICE, "HandleConflict for id=", this.inputFeature.id,);
        //handleDeleteVersionConflict
      }
    }
  }

  /**
   * @throws VersionConflictError, MergeConflictError
   */
  handleVersionConflict() {
    //plv8.elog(NOTICE, "Version conflict");
    return this.isDelete ? this.handleDeleteVersionConflict() : this.handleWriteVersionConflict();
  }

  /**
   * @throws VersionConflictError
   */
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
    throw new VersionConflictError(`Version conflict while trying to ${this._operation2HumanReadable(
        this.operation)} feature with ID ${this.inputFeature.id} in version ${this.version}.`)
    .withHint(`Base version ${this.baseVersion} is not matching the current HEAD version ${this._getFeatureVersion(
        this.loadFeature(this.inputFeature.id))}.`)
  }

  /**
   * This method will *only* be called in case a version conflict was detected and onVersionConflict == MERGE
   * @throws MergeConflictError If the both write diffs are not recursively disjunct and onMergeConflict == ERROR
   */
  mergeChanges() {
    plv8.elog(NOTICE, "MERGE changes", this.onVersionConflict, this.onMergeConflict); //TODO: Use debug logging
    let headFeature = this.loadFeature(this.inputFeature.id);
    let baseFeature = this.loadFeature(this.inputFeature.id, this.baseVersion);
    //TODO: Fix order of diff arguments
    let inputDiff = this.isPartial ? this.inputFeature : this.diff(baseFeature, this.inputFeature); //Our incoming change
    let headDiff = this.diff(baseFeature, headFeature); //The other change
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
    let error = new MergeConflictError(`Merge conflict while trying to ${this._operation2HumanReadable(
        this.operation)} feature with ID ${this.inputFeature.id} in version ${this.version}.`)
    .withHint(`Base version ${this.baseVersion} was not matching the current HEAD version ${this._getFeatureVersion(
        this.loadFeature(this.inputFeature.id))} and a merge was not possible.`);

    if (!this.isDelete) {
      let detail = `The following conflicts occurred for the feature with ID ${this.inputFeature.id}:`;
      for (let conflict of this.attributeConflicts) {
        let path = Object.getOwnPropertyNames(conflict)[0];
        detail += "\n\t" + `- ${path}: ${typeof conflict[path][0] == "string" ? `"${conflict[path][0]}"`
            : conflict[path][0]} <> ${typeof conflict[path][1] == "string" ? `"${conflict[path][1]}"` : conflict[path][1]}`
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

    let sql = `SELECT id, version, author, jsondata, geo::JSONB FROM "${this.schema}"."${this.table}"`;

    let res = version == "HEAD"
        //next_version + operation supports head retrieval if we have multiple versions
        ? plv8.execute(sql + "WHERE id = $1 AND next_version = max_bigint() AND operation != $2", id, "D")
        : plv8.execute(sql + "WHERE id = $1 AND version = $2 AND operation != $3", id, version, "D");

    if (res.length == 0)
      return null;
    else if (res.length == 1) {
      let feature = res[0].jsondata;
      feature.id = res[0].id;
      feature.geometry = res[0].geo;
      feature.properties[this.XYZ_NS].version = res[0].version;
      feature.properties[this.XYZ_NS].author = res[0].author;
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
    return feature.properties[this.XYZ_NS].version;
  }

  _hasDeletedFlag(feature) {
    return this.inputFeature.properties == undefined
    || this.inputFeature.properties[this.XYZ_NS] == undefined ?
        false : this.inputFeature.properties[this.XYZ_NS].deleted == true;
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
          //plv8.elog(NOTICE, "patch [", key, "]->", JSON.stringify(target[key]), "-", JSON.stringify(inputDiff[key]));
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
    this.baseVersion = feature.properties && feature.properties[this.XYZ_NS] && feature.properties[this.XYZ_NS].version;

    feature.id = feature.id || Math.random().toString(36).slice(2, 10);
    feature.type = feature.type || "Feature";
    this.author = this.author || "ANOYMOUS";
    feature.properties = feature.properties || {};
    feature.properties[this.XYZ_NS] = feature.properties[this.XYZ_NS] || {};

    //TODO: Set the createdAt TS right before writing and only if it is an insert
    let now = Date.now();
    feature.properties[this.XYZ_NS].createdAt = (feature.properties[this.XYZ_NS].createdAt == undefined) ? now
        : (feature.properties[this.XYZ_NS].createdAt);
    feature.properties[this.XYZ_NS].updatedAt = now;
    feature.properties[this.XYZ_NS].version = this.version;
    feature.properties[this.XYZ_NS].author = this.author;
  }
}

plv8.FeatureWriter = FeatureWriter;