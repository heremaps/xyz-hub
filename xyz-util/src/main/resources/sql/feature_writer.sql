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

	let result = {type: "FeatureCollection", features : []};
    for (let feature of features)
        result.features.push(writeFeature(feature, version, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial));
    return result;
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
    ${{Exception.js}}
    ${{FeatureWriter.js}}
    //Init completed

    //Run the actual command
    return new FeatureWriter(input_feature, version, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial).writeFeature();
$BODY$ LANGUAGE plv8 IMMUTABLE;