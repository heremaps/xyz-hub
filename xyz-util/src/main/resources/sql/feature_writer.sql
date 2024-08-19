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
    RETURNS JSONB AS
$BODY$

    //Import other functions
    const context = plv8.context = key => {
      let rows = plv8.execute("SELECT context($1)", key);
      if (!rows.length)
        return null;
      let results = rows[0].context;
      return results?.length > 0 ? results[0] : null;
    }; //TODO: find context function instead

    //Init block of internal feature_writer functionality
    ${{Exception.js}}
    ${{FeatureWriter.js}}
    //Init completed

    //Actual executions
    if (input_features == null)
        throw new Error("Parameter input_features must not be null.");

    return FeatureWriter.writeFeatures(JSON.parse(input_features), author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial);
$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @public
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_feature(input_feature TEXT, author TEXT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN)
    RETURNS JSONB AS $BODY$

    //Import other functions
    const writeFeatures = plv8.find_function("write_features");

    if (input_feature == null)
      throw new Error("Parameter input_feature must not be null.");

    return writeFeatures(`[${input_feature}]`, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial);
$BODY$ LANGUAGE plv8 IMMUTABLE;