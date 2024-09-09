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
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN, version BIGINT = NULL, return_result BOOLEAN = true)
    RETURNS JSONB AS
$BODY$

    //TODO: Check why / from where "NULL" strings are passed into here and remove that bloody workaround once the actual issue has been solved properly
    if(on_exists != null && on_exists.toLowerCase() == "null")
       on_exists = null;
    if(on_not_exists != null && on_not_exists.toLowerCase() == "null")
       on_not_exists = null;
    if(on_version_conflict != null && on_version_conflict.toLowerCase() == "null")
       on_version_conflict = null;
    if(on_merge_conflict != null && on_merge_conflict.toLowerCase() == "null")
       on_merge_conflict = null;

    //Import other functions
    let _queryContext;
    const _context = plv8.context = () => {
      let rows = plv8.execute("SELECT context()");
      return rows[0].context;
    };
    const queryContext = key => {
      if (_queryContext == null)
        _queryContext = _context();
      return _queryContext;
    };

    //Init block of internal feature_writer functionality
    ${{Exception.js}}
    ${{FeatureWriter.js}}
    //Init completed

    //Actual executions
    if (input_features == null)
      throw new Error("Parameter input_features must not be null.");

    let result = FeatureWriter.writeFeatures(JSON.parse(input_features), author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial, version == null ? undefined : version);

    return return_result ? result : {"count": result.features.length};
$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @public
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_feature(input_feature TEXT, author TEXT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN, version BIGINT = NULL, return_result BOOLEAN = true)
    RETURNS JSONB AS $BODY$

    //Import other functions
    const writeFeatures = plv8.find_function("write_features");

    if (input_feature == null)
      throw new Error("Parameter input_feature must not be null.");

    return writeFeatures(`[${input_feature}]`, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial, version, return_result);
$BODY$ LANGUAGE plv8 IMMUTABLE;