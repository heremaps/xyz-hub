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

class DatabaseWriter {

  schema;
  table;
  tableBaseVersion;

  tableLayout;
  /**
   * @type {boolean}
   */
  batchMode;
  /**
   * @type {{<string>: PreparedPlan}}
   */
  plans = {};
  /**
   * @type {{<string>: object[][]}}
   */
  parameterSets = {};
  /**
   * @type {{<string>: (function(object[]) : FeatureModificationExecutionResult)[]}}
   */
  resultParsers = {};

  constructor(schema, table, tableBaseVersion, batchMode = false, tableLayout) {
    this.schema = schema;
    this.table = table;
    this.tableBaseVersion = tableBaseVersion;
    this.batchMode = batchMode;
    this.tableLayout = tableLayout;
  }

  /**
   * Executes all prepared queries of this DatabaseWriter.
   * @returns {FeatureModificationExecutionResult[]}
   */
  execute() {
    let results = [];
    for (let method in this.plans)
      results = results.concat(this._executePlan(this.plans[method], this.parameterSets[method], this.resultParsers[method]));

    this.clear();
    return results;
  }

  clear() {
    this.plans = {};
    this.parameterSets = {};
    this.resultParsers = {}
  }

  /**
   * Executes a prepared query using the specified parameterSet and returns the results after parsing them using the
   * specified resultParser.
   * @param {PreparedPlan} plan
   * @param {object[][]} parameterSet
   * @param {(function(object[]) : FeatureModificationExecutionResult)[]} resultParsers
   * @returns {FeatureModificationExecutionResult[]}
   * @private
   */
  _executePlan(plan, parameterSet, resultParsers) {
    let results = [];
    for (let index in parameterSet) {
      let result = resultParsers[index](plan.execute(parameterSet[index]));
      if (result != null)
        results.push(result);
    }
    plan.free();
    return results;
  }

  /**
   * Creates a prepared plan / query that can be executed multiple times for different parameters.
   * Using these prepared statements improves the performance of queries that are executed many times in the same way, but with
   * different parameters.
   * @param {string} sql
   * @param {string[]} typeNames
   * @returns {PreparedPlan}
   * @private
   */
  _preparePlan(sql, typeNames) {
    return plv8.prepare(sql, typeNames);
  }

  /**
   * @param {function(FeatureModificationExecutionResult) : FeatureModificationExecutionResult} resultHandler
   * @returns {FeatureModificationExecutionResult}
   */
  upsertRow(inputFeature, version, operation, author, onExists, resultHandler) {
    let uniqueConstraintExists = queryContext().uniqueConstraintExists !== false;
    this.enrichTimestamps(inputFeature, true);
    let onConflict = onExists == "REPLACE" && uniqueConstraintExists
        ? ` ON CONFLICT (id, next_version) DO UPDATE SET
          version = greatest(tbl.version, EXCLUDED.version),
          operation = CASE WHEN $3 = 'H' THEN 'J' ELSE 'U' END,
          author = EXCLUDED.author,
          jsondata = jsonb_set(EXCLUDED.jsondata, '{properties, ${XYZ_NS}, createdAt}',
                 tbl.jsondata->'properties'->'${XYZ_NS}'->'createdAt'),
          geo = EXCLUDED.geo
          WHERE EXCLUDED.jsondata #- ARRAY['properties', '${XYZ_NS}'] IS DISTINCT FROM tbl.jsondata #- ARRAY['properties', '${XYZ_NS}']`
        : onExists == "RETAIN" ? " ON CONFLICT(id, next_version) DO NOTHING" : "";

    let sql = `INSERT INTO "${this.schema}"."${this.table}" AS tbl
                        (id, version, operation, author, jsondata, geo)
                        VALUES ($1, $2, $3, $4, $5::JSONB - 'geometry', CASE WHEN $6::JSONB IS NULL THEN NULL
                            ELSE xyz_reduce_precision(ST_Force3D(ST_GeomFromGeoJSON($6::JSONB)), false) END) ${onConflict}
                        RETURNING (jsondata->'properties'->'${XYZ_NS}'->'createdAt') as created_at, operation`;

    //sql += " RETURNING COALESCE(jsonb_set(jsondata,'{geometry}',ST_ASGeojson(geo)::JSONB) as feature)";

    let method = "upsertRow";
    if (!this.plans[method]) {
      this.plans[method] = this._preparePlan(sql, ["TEXT", "BIGINT", "CHAR", "TEXT", "JSONB", "JSONB"]);
      this.parameterSets[method] = [];
      this.resultParsers[method] = [];
    }

    if (inputFeature == null) {
      plv8.elog(NOTICE, `FW_LOG [${queryContext().queryId}] Can not write a feature that is null`);
      throw new XyzException("Can not write a feature that is null");
    }

    this.parameterSets[method].push([
      inputFeature.id,
      version,
      operation,
      author,
      inputFeature,
      inputFeature.geometry //TODO: Use TEXT
    ]);
    this.resultParsers[method].push(result => {
      //TODO: In future return null here, if actually no update was taking place (due to non-changed content) - keeping status quo for BWC for now
      let executedAction = !result.length ? ExecutionAction.NONE : ExecutionAction.fromOperation[result[0].operation];
      if (result.length && executedAction == ExecutionAction.UPDATED)
        //Inject createdAt
        inputFeature.properties[XYZ_NS].createdAt = result[0].created_at;
      return resultHandler(new FeatureModificationExecutionResult(executedAction, inputFeature, version, author));
    });

    if (!this.batchMode)
      return this.execute()[0];
  }

  /**
   * @param {function(FeatureModificationExecutionResult) : FeatureModificationExecutionResult} resultHandler
   * @returns {FeatureModificationExecutionResult}
   */
  updateRow(inputFeature, version, author, baseVersion, resultHandler) {
    this.enrichTimestamps(inputFeature);
    let sql = `UPDATE "${this.schema}"."${this.table}" AS tbl
                         SET version   = $1,
                             operation = $2,
                             author    = $3,
                             jsondata  = jsonb_set($4::JSONB - 'geometry', '{properties, ${XYZ_NS}, createdAt}',
                                                   tbl.jsondata -> 'properties' -> '${XYZ_NS}' -> 'createdAt'),
                             geo       = CASE WHEN $5::JSONB IS NULL THEN NULL ELSE xyz_reduce_precision(ST_Force3D(ST_GeomFromGeoJSON($5::JSONB)), false) END
                         WHERE id = $6
                           AND version = $7
                         RETURNING (jsondata -> 'properties' -> '${XYZ_NS}' -> 'createdAt') as created_at, operation`;

    let method = "updateRow";
    if (!this.plans[method]) {
      this.plans[method] = this._preparePlan(sql, ["BIGINT", "CHAR", "TEXT", "JSONB", "JSONB", "TEXT", "BIGINT"]);
      this.parameterSets[method] = [];
      this.resultParsers[method] = [];
    }

    if (inputFeature == null) {
      plv8.elog(NOTICE, `FW_LOG [${queryContext().queryId}] Can not write a feature that is null`);
      throw new XyzException("Can not write a feature that is null");
    }

    this.parameterSets[method].push([
      version,
      "U" /*TODO set version operation*/,
      author,
      inputFeature,
      inputFeature.geometry, //TODO: Use TEXT
      inputFeature.id,
      baseVersion
    ]);
    this.resultParsers[method].push(result => {
      return resultHandler(!result.length ? null : new FeatureModificationExecutionResult(ExecutionAction.UPDATED, inputFeature, version, author));
    });

    if (!this.batchMode)
      return this.execute()[0];
  }

  /**
   * @param {function(FeatureModificationExecutionResult) : FeatureModificationExecutionResult} resultHandler
   * @returns {FeatureModificationExecutionResult}
   */
  insertHistoryRow(inputFeature, baseFeature, version, operation, author, resultHandler) {
    //TODO: Improve performance by reading geo inside JS and then pass it separately and use TEXT / WKB / BYTEA
    this.enrichTimestamps(inputFeature, true);
    let extraCols = '';
    let extraVals = '';

    if (this.tableLayout === 'NEW_LAYOUT') {
      extraCols = ', searchable';
      extraVals = ', $8::JSONB ';
    }

    const sql = `INSERT INTO "${this.schema}"."${this.table}"
                (id, version, operation, author, jsondata, geo ${extraCols})
                 VALUES ($1, $2, $3, $4,
                         $5::JSONB - 'geometry',
                         CASE WHEN $6::JSONB IS NULL THEN
                            NULL
                        ELSE
                            xyz_reduce_precision(ST_Force3D(ST_GeomFromGeoJSON($6::JSONB)), false)
                        END ${extraVals}
            )`;

    this._createHistoryPartition(version);
    this._purgeOldChangesets(version);

    let method = "insertHistoryRow";
    if (!this.plans[method]) {
      const paramTypes = ["TEXT", "BIGINT", "CHAR", "TEXT", "JSONB", "JSONB"];

      if (this.tableLayout === 'NEW_LAYOUT') {
        paramTypes.push("JSONB");
      }

      this.plans[method] = this._preparePlan(sql, paramTypes);
      this.parameterSets[method] = [];
      this.resultParsers[method] = [];
    }

    if (inputFeature == null) {
      plv8.elog(NOTICE, `FW_LOG [${queryContext().queryId}] Can not write a feature that is null`);
      throw new XyzException("Can not write a feature that is null");
    }

    let createdAtFromExistingFeature = (baseFeature && baseFeature.properties[XYZ_NS].createdAt)?
      baseFeature.properties[XYZ_NS].createdAt : -1;

    const params = [
      inputFeature.id,
      version,
      operation,
      author,
      inputFeature,
      inputFeature.geometry
    ];

    if (this.tableLayout === 'NEW_LAYOUT') {
      const searchable = {
        refQuad: inputFeature.properties.refQuad,
        globalVersion: inputFeature.properties.globalVersion
      };
      params.push(searchable);
    }

    this.parameterSets[method].push(params);
    this.resultParsers[method].push(result => {
      let executedAction = inputFeature.properties[XYZ_NS].deleted ? ExecutionAction.DELETED : ExecutionAction.fromOperation[operation];
      return resultHandler(new FeatureModificationExecutionResult(executedAction, inputFeature, version + this.tableBaseVersion, author));
    });

    if (!this.batchMode)
      return this.execute()[0];
  }

  _PARTITION_SIZE() {
    return queryContext().PARTITION_SIZE || 100000; //TODO: Ensure the partition size is always set in the query context
  }

  /**
   * If the current history partition is nearly full, create the next one already
   * @param version The version that is about to be written
   * @private
   */
  _createHistoryPartition(version) {
    const PARTITION_SIZE = this._PARTITION_SIZE();
    if (version % PARTITION_SIZE > PARTITION_SIZE - 50)
      plv8.execute(`SELECT xyz_create_history_partition($1, $2, $3::BIGINT, $4::BIGINT);`,
          [this.schema, this.table, Math.floor(version / PARTITION_SIZE) + 1, PARTITION_SIZE]);
  }

  _purgeOldChangesets(version) {
    const PARTITION_SIZE = this._PARTITION_SIZE();
    let minVersion = queryContext().minVersion;
    let pw = queryContext().pw;
    let versionsToKeep = queryContext().versionsToKeep;

    if (!pw)
      //TODO: Ensure that all necessary queryContext fields are set from all places
      return;

    if (version % 1000 == 0) {
      let minAvailableVersion = version - versionsToKeep + 1;
      if (minVersion != -1)
        minVersion = Math.min(minVersion, minAvailableVersion);
      else
        minVersion = minAvailableVersion;

      if (minVersion > 0)
        plv8.execute(`SELECT xyz_delete_changesets_async($1, $2, $3::BIGINT, $4::BIGINT, $5);`,
            [this.schema, this.table, PARTITION_SIZE, minVersion, pw]);
    }
  }

  /**
   * @param {function(FeatureModificationExecutionResult) : FeatureModificationExecutionResult} resultHandler
   * @throws VersionConflictError
   * @returns {FeatureModificationExecutionResult}
   */
  deleteRow(inputFeature, version, author, onVersionConflict, baseVersion, resultHandler) {
    baseVersion = onVersionConflict == null ? -1 : baseVersion;

    let sql = `DELETE FROM "${this.schema}"."${this.table}" WHERE id = $1 `;
    let parameters = [inputFeature.id];
    let parameterTypes = ["TEXT"];


    let method;
    if (baseVersion == 0) {
      //TODO: Check if this case is necessary. Why would we need to delete sth. on a space with v=0 (empty space) [TODO: compare with old impl in xyz_simple_delete()]
      sql += "AND next_version = " + MAX_BIG_INT + ";"
      method = "_deleteRow1";
    }
    else if (baseVersion > 0) {
      sql += "AND version = $2;";
      parameters.push(baseVersion);
      parameterTypes.push("BIGINT");
      method = "_deleteRow2";
    }
    else
      method = "_deleteRow3";

    if (!this.plans[method]) {
      this.plans[method] = this._preparePlan(sql, parameterTypes);
      this.parameterSets[method] = [];
      this.resultParsers[method] = [];
    }

    this.parameterSets[method].push(parameters);
    this.resultParsers[method].push(deletedRows => {
      if (deletedRows == 0) {
        if (onVersionConflict != null) {
          plv8.elog(NOTICE, "FW_LOG HandleConflict for deletion of id: " + inputFeature.id);
          //handleDeleteVersionConflict //TODO: throw some conflict error here that can be caught & handled by FeatureWriter
          return null;
        }
        this._throwFeatureNotExistsError(inputFeature.id);
      }
      return resultHandler(new FeatureModificationExecutionResult(ExecutionAction.DELETED, inputFeature, version, author));
    });

    if (!this.batchMode)
      return this.execute()[0];
  }

  enrichTimestamps(feature, isCreation = false, baseFeature = null) {
    let now = Date.now();
    feature.properties = {
      ...feature.properties,
      [XYZ_NS]: {
        ...feature.properties[XYZ_NS],
        updatedAt: now
      }
    };
    if (isCreation)
      feature.properties[XYZ_NS].createdAt = now;
    else if (baseFeature)
      feature.properties[XYZ_NS].createdAt = baseFeature.properties[XYZ_NS].createdAt ? baseFeature.properties[XYZ_NS].createdAt : -1;
  }

  _throwFeatureNotExistsError(featureId) {
    throw new FeatureNotExistsException(`Feature with ID ${featureId} not exists!`);
  }
}

class FeatureModificationExecutionResult {
  action;
  feature;

  constructor(action, feature, version, author) {
    this.action = action;
    this.feature = feature;
    this.enrichResultFeature(version, author);
  }

  enrichResultFeature(version, author) {
    this.feature.properties[XYZ_NS].version = version;
    this.feature.properties[XYZ_NS].author = author;
  }
}

plv8.DatabaseWriter = DatabaseWriter;

if (plv8.global) {
  global.DatabaseWriter = DatabaseWriter;
  global.FeatureModificationExecutionResult = FeatureModificationExecutionResult;
}