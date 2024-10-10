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
CREATE OR REPLACE FUNCTION write_features_old(input_features TEXT, author TEXT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN, version BIGINT = NULL, return_result BOOLEAN = true)
    RETURNS JSONB AS
$BODY$
    const writeFeatures = plv8.find_function("write_features");

    if (input_features == null)
      throw new Error("Parameter input_features must not be null.");

    let modification = `{
        "updateStrategy": {
            "onExists": ${JSON.stringify(on_exists)},
            "onNotExists": ${JSON.stringify(on_not_exists)},
            "onVersionConflict": ${JSON.stringify(on_version_conflict)},
            "onMergeConflict": ${JSON.stringify(on_merge_conflict)}
        },
        "featureData": {"type": "FeatureCollection", "features": ${input_features}},
        "partialUpdates": ${is_partial}
    }`;

    return writeFeatures(`[${modification}]`, author, return_result, version == null ? undefined : version);
$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @public
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_features(feature_modifications TEXT, author TEXT, return_result BOOLEAN = true, version BIGINT = NULL)
    RETURNS JSONB AS
$BODY$
    try {
      //Actual executions
      if (feature_modifications == null)
        throw new Error("Parameter feature_modifications must not be null.");

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

      let result = FeatureWriter.writeFeatureModifications(JSON.parse(feature_modifications), author, version == null ? undefined : version);

      return return_result ? result : {"count": result.features.length};
    }
    catch (error) {
      if (!error.code)
        throw new Error("Unexpected error in feature_writer: " + error.message);
      else
        throw error;
    }
$BODY$ LANGUAGE plv8 VOLATILE;

/**
 * @public
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_feature(input_feature TEXT, author TEXT, on_exists TEXT,
    on_not_exists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN, version BIGINT = NULL, return_result BOOLEAN = true)
    RETURNS JSONB AS $BODY$

    //Import other functions
    const writeFeatures = plv8.find_function("write_features_old");

    if (input_feature == null)
      throw new Error("Parameter input_feature must not be null.");

    return writeFeatures(`[${input_feature}]`, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial, version == null ? undefined : version, return_result);
$BODY$ LANGUAGE plv8 VOLATILE;