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

const MAX_BIG_INT = "9223372036854775807"; //NOTE: Must be a string because of JS precision
const XYZ_NS = "@ns:com:here:xyz";
const FW_BATCH_MODE = () => !!queryContext().batchMode; //true;
const IDENTITY_HANDLER = r => r;

/**
 * The unified implementation of the database-based feature writer.
 */
class FeatureWriter {
  debugOutput = false; //TODO: Read from queryContext

  //Context input fields
  schema;
  tables;
  context;
  historyEnabled;

  //Process input fields
  inputFeature;
  version;
  author;
  isPartial;
  baseVersion;
  featureHooks;

  onExists;
  onNotExists;
  onVersionConflict;
  onMergeConflict;

  batchMode = FW_BATCH_MODE();
  static dbWriter;

  //Process generated / tmp fields
  headFeature;
  operation = "I";
  isDelete = false;
  attributeConflicts;
  ignoreConflictPaths = {
    [`properties.${XYZ_NS}.author`]: true,
    [`properties.${XYZ_NS}.version`]: true,
    [`properties.${XYZ_NS}.createdAt`]: true,
    [`properties.${XYZ_NS}.updatedAt`]: true
  };

  constructor(inputFeature, version, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, featureHooks) {
    // if (isPartial && onNotExists == "CREATE")
    //   throw new IllegalArgumentException("onNotExists must not be \"CREATE\" for partial writes.");

    this.schema = queryContext().schema;
    this.tables = queryContext().tables;
    this.context = queryContext().context;
    this.historyEnabled = queryContext().historyEnabled;

    this.inputFeature = inputFeature;
    this.version = version;
    this.author = author || "ANONYMOUS";
    this.baseVersion = (this.inputFeature.properties || {})[XYZ_NS]?.version;
    this.featureHooks = featureHooks;
    this.enrichFeature();

    this.isDelete = !!this.inputFeature.properties[XYZ_NS].deleted;
    this.onExists = onExists || "REPLACE";
    this.onNotExists = onNotExists || (isPartial ? "ERROR" : "CREATE");
    this.onVersionConflict = onVersionConflict == null ? null : onVersionConflict || (this.isDelete ? "REPLACE" : "MERGE");
    this.onMergeConflict = onMergeConflict == null ? "ERROR" : onMergeConflict;
    this.isPartial = isPartial;

    if (this.onVersionConflict == "MERGE" && !this.historyEnabled)
      throw new IllegalArgumentException("MERGE can not be executed for spaces without history!");

    if (this.onVersionConflict && this.baseVersion == null)
      throw new IllegalArgumentException("The provided Feature does not have a baseVersion but a version conflict detection was requested!");

    if (this.debugOutput)
      this.debugBox(JSON.stringify(this, null, 2));
  }

  /**
   * @throws VersionConflictError, MergeConflictError, FeatureExistsError
   * @returns {FeatureModificationExecutionResult}
   */
  writeFeature() {
    return this.isDelete ? this.deleteFeature() : this.writeRow();
  }

  /**
   * @throws VersionConflictError, MergeConflictError
   * @returns {FeatureModificationExecutionResult}
   */
  deleteFeature() {
    if (this.context == "DEFAULT") {
      if (!this.historyEnabled && this.featureExistsInHead(this.inputFeature.id) && !this.featureExistsInHead(this.inputFeature.id, "SUPER"))
        return this.deleteRow();

      this.onExists = "REPLACE";
      this.onNotExists = "CREATE";
      this.operation = "H";
      this._transformToDeletedFeature();
      return this.writeRow();
    }
    else if (this.context == null || this.context == "EXTENSION" || this.context == "SUPER") {
      if (this.historyEnabled) {
        //TODO: Only insert deletion row if the object actually exists in HEAD
        this.onExists = "DELETE";
        this.onNotExists = this.onNotExists == "ERROR" ? this.onNotExists : "RETAIN";
        this._transformToDeletedFeature();
        return this.writeRow();
      }
      return this.deleteRow();
    }
    else
      throw new IllegalArgumentException(`Unsupported context for feature deletion: ${this.context}`);
  }

  _transformToDeletedFeature() {
    this.inputFeature = FeatureWriter._transformToDeletedFeature(this.inputFeature.id);
  }

  static _transformToDeletedFeature(featureId) {
    return {type: "Feature", id: featureId, properties: {[XYZ_NS]: {deleted: true}}};
  }

  /**
   * @throws VersionConflictError, MergeConflictError, FeatureExistsError
   * @returns {FeatureModificationExecutionResult}
   */
  writeRow() {
    if (this.historyEnabled)
      return this.writeRowWithHistory();
    return this.writeRowWithoutHistory();
  }

  /**
   * @throws VersionConflictError, MergeConflictError, FeatureExistsError
   * @returns {FeatureModificationExecutionResult}
   */
  writeRowWithHistory() {
    this.inputFeature = this.patchToHeadIfPartial();
    if (this.inputFeature == null)
      return null;

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
      let updatedRows = this._updateNextVersion();
      if (updatedRows.length == 1) {
        if (this.onExists == "DELETE")
          this.operation = "D";

        if (this.operation == "D") {
          if (updatedRows[0].operation != "D") {
            this._transformToDeletedFeature();
            return this._insertHistoryRow();
          }
        }
        else {
          this.operation = updatedRows[0].operation == "D" ? this.operation : this._transformToUpdate(this.operation);
          return this._insertHistoryRow();
        }
      }
      else {
        if (this.loadFeature(this.inputFeature.id) != null)
          //The feature exists in HEAD, and still no previous version was updated, so we have a version conflict
          return this.handleVersionConflict();
        else {
          if (updatedRows[0]?.operation != "D")
            return this._insertHistoryRow();
        }
      }
    }
    else {
      //Version conflict detection is not active
      let updatedRows = this._updateNextVersion();
      if (updatedRows.length == 1) {
        if (this.onExists == "DELETE")
          this.operation = "D";

        if (this.operation == "D") {
          if (updatedRows[0].operation != "D") {
            if (this.context == "DEFAULT" && this.featureExistsInHead(this.inputFeature.id, "SUPER"))
              this.operation = "J";
            this._transformToDeletedFeature();
            return this._insertHistoryRow();
          }
        }
        else {
          this.operation = updatedRows[0].operation == "D" ? this.operation : this._transformToUpdate(this.operation);
          return this._insertHistoryRow();
        }
      }
      else {
        switch (this.onNotExists) {
          case "CREATE":
            break; //NOTHING TO DO;
          case "ERROR":
            this._throwFeatureNotExistsError();
          case "RETAIN":
            return null;
        }

        /*
        If the space is a composite space a not existence in the extension does not necessarily mean,
        that the feature does not exist at all.
        It could exist in the SUPER space.
         */
        if (this.context == "DEFAULT") {
          if (this.onExists == "DELETE")
            return this.deleteFeature();
        }

        if (this.operation != "D")
          return this._insertHistoryRow();
      }
    }
  }

  /**
   * Patches the incoming partial inputFeature into the current HEAD feature
   * @throws FeatureNotExistsException if the features does not exist in HEAD and onNotExists = ERROR
   * @returns {Feature|null} The resulting feature, or null if the feature does not exist in HEAD and onNotExists = RETAIN
   */
  patchToHeadIfPartial() {
    if (this.isPartial) {
      let headFeature = this.loadFeature(this.inputFeature.id);
      if (headFeature == null) {
        switch (this.onNotExists) {
          case "RETAIN":
            return null;
          case "CREATE":
            headFeature = this.newEmptyFeature();
            break;
          case "ERROR":
            this._throwFeatureNotExistsError();
        }
      }
      return this.patch(headFeature, this.inputFeature);
    }
    else
      this.removeNullValues(this.inputFeature);
    return this.inputFeature;
  }

  _transformToUpdate(operation) {
    return operation == "I" || operation == "U" ? "U" : "J";
  }

  _operation2HumanReadable(operation) {
    return operation == "I" || operation == "H" ? "insert" : operation == "U" || operation == "J" ? "update" : "delete";
  }

  /**
   * @throws FeatureExistsError
   * @returns {FeatureModificationExecutionResult}
   */
  writeRowWithoutHistory() {
    this.inputFeature = this.patchToHeadIfPartial();
    if (this.inputFeature == null)
      return null;

    if (this.onExists == "DELETE")
      //TODO: Ensure deletion is done with conflict detection (depending on this.onVersionConflict)
      return this.deleteFeature();

    if (this.onExists != "REPLACE" || this.onNotExists != "CREATE") {
      let headFeature = this.loadFeature(this.inputFeature.id);

      switch (this.onExists) {
        case "RETAIN":
          if (headFeature != null)
            return null;
          break;
        case "ERROR":
          if (headFeature != null)
            this._throwFeatureExistsError();
          //NOT handling REPLACE
      }

      switch (this.onNotExists) {
        case "RETAIN":
          if (headFeature == null)
            return null;
          break;
        case "ERROR":
          if (headFeature == null)
            this._throwFeatureNotExistsError();
          //NOT handling CREATE
      }
    }

    if (this.onVersionConflict != null) {
      this.debugBox("Version conflict handling! Base version: " + this.baseVersion);

      let execution = this._updateRow();
      if (execution == null)
        return this.handleVersionConflict();
      return execution;
    }
    else {
      try {
        if (this.onNotExists == "RETAIN" && !this.featureExistsInHead(this.inputFeature.id))
          return null;

        let execution = this._upsertRow(execution => {
          if (execution.action != ExecutionAction.UPDATED && this.onNotExists == "ERROR")
            this._throwFeatureNotExistsError();
          return execution;
        });

        return execution;
      }
      catch (e) {
        if (e.sqlerrcode == SQLErrors.CONFLICT) {
          if (this.onExists == "ERROR")
            this._throwFeatureExistsError();
          else
            throw new XyzException("Unexpected conflict while trying to perform write operation.", e).withDetail(e.detail).withHint(e.hint);
        }

        this.debugBox(e.stack);

        //Rethrow the original error, as it is unexpected
        throw e;
      }
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
   * @returns {FeatureModificationExecutionResult}
   */
  deleteRow(resultHandler = IDENTITY_HANDLER) {
    //TODO: do we need the payload of the feature as return?
    //base_version get provided from user (extend api endpoint if necessary)
    this.debugBox("Delete feature with id: " + this.inputFeature.id);
    return FeatureWriter.dbWriter.deleteRow(this.inputFeature, this.version, this.author, this.onVersionConflict, this.baseVersion,
        resultHandler);
  }

  /**
   * @throws VersionConflictError, MergeConflictError
   * @returns {FeatureModificationExecutionResult}
   */
  handleVersionConflict() {
    return this.isDelete ? this.handleDeleteVersionConflict() : this.handleWriteVersionConflict();
  }

  /**
   * @throws VersionConflictError
   * @returns {FeatureModificationExecutionResult}
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
   * @returns {FeatureModificationExecutionResult}
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
    this.debugBox("MERGE changes: onVersionConflict(" + this.onVersionConflict + ") onMergeConflict(" +  this.onMergeConflict + ")");
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

  featureExistsInHead(id, context = this.context) {
    //TODO: Check existence without actually loading feature data
    return !!this.loadFeature(id, "HEAD", context);
  }

  newEmptyFeature() {
    return {type: "Feature"};
  }

  loadFeature(id, version = "HEAD", context = this.context) {
    if (version == "HEAD" && context == this.context && this.headFeature) //NOTE: Only cache for the defaults
      return this.headFeature;

    //TODO: Check if we need a check on operation.
    if (id == null)
      return null;

    let tables = context == "EXTENSION" ? this.tables.slice(-1) : context == "SUPER" ? this.tables.slice(0, -1) : this.tables;
    let res = this._loadFeature(id, version, tables);

    if (!res.length)
      return null;
    else if (res.length == 1) {
      let feature = res[0].jsondata;
      feature.id = res[0].id;
      feature.geometry = res[0].geo;
      feature.properties[XYZ_NS].version = res[0].version;

      //Cache the HEAD-feature in case its needed later again
      if (version == "HEAD" && context == this.context) //NOTE: Only cache for the defaults
        this.headFeature = feature;
      return feature;
    }
    else
        //Unexpected exception:
      throw new XyzException("Found two Features with the same id!");
  }

  _getFeatureVersion(feature) {
    return feature.properties[XYZ_NS].version;
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
    target = target || {};
    for (let key in inputDiff) {
      if (inputDiff.hasOwnProperty(key)) {
        if (inputDiff[key] === null)
          delete target[key];
        else if (typeof inputDiff[key] == "object" && !Array.isArray(inputDiff[key]) && inputDiff[key] !== null) {
          if (!target[key])
            target[key] = {};
          target[key] = this.patch(target[key], inputDiff[key]);
        }
        else
          target[key] = inputDiff[key];
      }
    }
    return target;
  }

  removeNullValues(obj) {
    for (let key in obj)
      if (obj[key] === null)
        delete obj[key];
      else if (!Array.isArray(obj[key]) && typeof obj[key] == "object")
        this.removeNullValues(obj[key]);
  }

  _randomAlphaNumeric(length) {
    let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    return [...Array(length)].reduce(c => c + alphabet[~~(Math.random() * alphabet.length)], "");
  }

  enrichFeature() {
    let feature = this.inputFeature;

    feature.id = feature.id || this._randomAlphaNumeric(16);
    feature.type = feature.type || "Feature";
    delete feature.bbox;
    feature.properties = feature.properties || {};
    feature.properties[XYZ_NS] = feature.properties[XYZ_NS] || {};
    this.featureHooks && this.featureHooks.forEach(featureHook => featureHook(feature));
  }

  debugBox(message) {
    if (!this.debugOutput)
      return;

    let width = 140;
    let leftRightBuffer = 2;
    let maxLineLength = width - leftRightBuffer * 2;
    let lines = message.split("\n").map(line => !line ? line : line.match(new RegExp(`.{1,${maxLineLength}}`, "g"))).flat();
    let longestLine = lines.map(line => line.length).reduce((a, b) => Math.max(a, b), -Infinity);
    let leftPadding = new Array(Math.floor((width - longestLine) / 2)).join(" ");
    lines = lines.map(line => "#" + leftPadding + line
        + new Array(width - leftPadding.length - line.length - 1).join(" ") + "#");
    if(this.debugOutput)
      plv8.elog(NOTICE, "\n" + new Array(width + 1).join("#") + "\n" + lines.join("\n") + "\n" + new Array(width + 1).join("#"));
  }

  //Low level DB / table facing helper methods:

  static getNextVersion() {
    const VERSION_SEQUENCE_SUFFIX = "_version_seq";
    let fullQualifiedSequenceName = `"${queryContext().schema}"."${(this._targetTable() + VERSION_SEQUENCE_SUFFIX)}"`;
    return plv8.execute("SELECT nextval($1)", [fullQualifiedSequenceName])[0].nextval;
  }

  static _targetTable(context = queryContext().context) {
    return queryContext().tables[queryContext().tables.length - (context == "SUPER" ? 2 : 1)];
  }

  _targetTable(context = this.context) {
    return FeatureWriter._targetTable(context);
  }

  _loadFeature(id, version, tables) {
    let tableAliases = tables.map((table, i) => "t" + (tables.length - i - 1));
    let whereCondition = `WHERE id = $1 AND ${version == "HEAD" ? "next_version" : "version"} = $2 AND operation != $3`;

    let sql = `
        SELECT
            id_array[index] AS id,
            version_array[index] AS version,
            author_array[index] AS author,
            jsondata_array[index] AS jsondata,
            ST_AsGeojson(geo_array[index])::JSONB AS geo
        FROM (
            SELECT
                ARRAY[${tableAliases.map(alias => alias + ".id").join(", ")}] AS id_array,
                ARRAY[${tableAliases.map(alias => alias + ".version").join(", ")}] AS version_array,
                ARRAY[${tableAliases.map(alias => alias + ".author").join(", ")}] AS author_array,
                ARRAY[${tableAliases.map(alias => alias + ".jsondata").join(", ")}] AS jsondata_array,
                ARRAY[${tableAliases.map(alias => alias + ".geo").join(", ")}] AS geo_array,
                coalesce_subscript(ARRAY[${tableAliases.map(alias => alias + ".id").join(", ")}]) AS index
            FROM (SELECT * FROM "${this.schema}"."${tables[tables.length - 1]}" ${whereCondition}) AS ${tableAliases[0]}
                ${tables.slice(0, -1).reverse().map((baseTable, i) => `FULL JOIN (SELECT * FROM "${this.schema}"."${baseTable}" ${whereCondition}) AS ${tableAliases[i + 1]} USING (id) `).join("\n")}
        );
    `;

    return plv8.execute(sql, [id, version == "HEAD" ? MAX_BIG_INT : version, "D"]);
  }

  /**
   * @private
   * @returns {FeatureModificationExecutionResult}
   */
  _insertHistoryRow(resultHandler = IDENTITY_HANDLER) {
    return FeatureWriter.dbWriter.insertHistoryRow(this.inputFeature, this.version, this.operation, this.author, resultHandler);
  }

  _updateNextVersion() {
    if (this.onVersionConflict != null)
      return plv8.execute(`UPDATE "${this.schema}"."${this._targetTable()}"
                           SET next_version = $1
                           WHERE id = $2
                             AND next_version = $3::BIGINT
                             AND (version = $4 OR operation = 'D' AND version < $1)
                           RETURNING *`, [this.version, this.inputFeature.id, MAX_BIG_INT, this.baseVersion]);
    else
      return plv8.execute(`UPDATE "${this.schema}"."${this._targetTable()}"
                           SET next_version = $1
                           WHERE id = $2
                             AND next_version = $3::BIGINT
                             AND version < $1
                           RETURNING *`, [this.version, this.inputFeature.id, MAX_BIG_INT]);
  }

  /**
   * @private
   * @returns {FeatureModificationExecutionResult}
   */
  _upsertRow(resultHandler = IDENTITY_HANDLER) {
    return FeatureWriter.dbWriter.upsertRow(this.inputFeature, this.version, this.operation, this.author, this.onExists, resultHandler);
  }

  /**
   * @private
   * @returns {FeatureModificationExecutionResult}
   */
  _updateRow(resultHandler = IDENTITY_HANDLER) {
    return FeatureWriter.dbWriter.updateRow(this.inputFeature, this.version, this.author, this.baseVersion, resultHandler);
  }

  static combineResults(featureCollections) {
    if (featureCollections.length <= 1)
      return featureCollections[0];

    let result = featureCollections[0];
    for (let i = 1; i < featureCollections.length; i++) {
      result.features = result.features.concat(featureCollections[i].features);
      result.inserted = result.inserted.concat(featureCollections[i].inserted);
      result.updated = result.updated.concat(featureCollections[i].updated);
      result.deleted = result.deleted.concat(featureCollections[i].deleted);
    }

    return result;
  }

  static toFeatureList(featureModification) {
    return featureModification.featureData
        ? featureModification.featureData.features
        : featureModification.featureIds.map(featureId => FeatureWriter._transformToDeletedFeature(featureId));
  }

  /**
   * @returns {FeatureCollection}
   */
  static writeFeatures(inputFeatures, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, featureHooks, version = FeatureWriter.getNextVersion()) {
    FeatureWriter.dbWriter = new DatabaseWriter(queryContext().schema, FeatureWriter._targetTable(), FW_BATCH_MODE());
    let result = this.newFeatureCollection();
    for (let feature of inputFeatures) {
      let execution = new FeatureWriter(feature, version, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, featureHooks).writeFeature();
      this._collectResult(execution, result);
    }

    if (FW_BATCH_MODE()) {
      let executions = FeatureWriter.dbWriter.execute()
      for (let execution of executions)
        this._collectResult(execution, result);
    }

    return result;
  }

  static _collectResult(execution, result) {
    if (execution != null) {
      if (execution.action != ExecutionAction.DELETED)
        result.features.push(execution.feature);
      result[execution.action].push(execution.feature.id);
    }
  }

  static newFeatureCollection() {
    return {
      type: "FeatureCollection",
      features: [],
      inserted: [],
      updated: [],
      deleted: []
    };
  }

  /**
   * @returns {FeatureCollection}
   */
  static writeFeatureModifications(featureModifications, author, version = FeatureWriter.getNextVersion()) {
    let featureCollections = featureModifications.map(modification => FeatureWriter.writeFeatures(this.toFeatureList(modification),
        author, modification.updateStrategy.onExists, modification.updateStrategy.onNotExists,
        modification.updateStrategy.onVersionConflict, modification.updateStrategy.onMergeConflict, modification.partialUpdates,
        modification.featureHooks && modification.featureHooks.map(hook => eval(hook)), version));
    return this.combineResults(featureCollections);
  }

  /**
   * @returns {FeatureCollection}
   */
  static writeFeature(inputFeature, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, featureHooks, version = undefined) {
    return FeatureWriter.writeFeatures([inputFeature], author, onExists, onNotExists, onVersionConflict, onMergeConflict,
        isPartial, featureHooks, version);
  }
}

class ExecutionAction {
  static INSERTED = "inserted";
  static UPDATED = "updated";
  static DELETED = "deleted";

  static fromOperation = {
    "I": this.INSERTED,
    "H": this.INSERTED,
    "U": this.UPDATED,
    "J": this.UPDATED,
    "D": this.DELETED
  }
};

plv8.FeatureWriter = FeatureWriter;

if (plv8.global) {
  global.FeatureWriter = FeatureWriter;
  global.ExecutionAction = ExecutionAction;
  global.MAX_BIG_INT = MAX_BIG_INT;
  global.XYZ_NS = XYZ_NS;
}