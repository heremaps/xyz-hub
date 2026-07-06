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

CREATE EXTENSION IF NOT EXISTS plv8;

/**
 * @public
 * @param {string} json_input The actual payload(s) (of the type that is specified by parameter input_type) to be written by the FeatureWriter
 * @param {string} input_type The type of the JSON input. Possible values: "FeatureCollection", "Features", "Feature", "Modifications"
 * @param {string} author The ID of the user that is committing the change
 * @param {boolean} return_result = false Whether to return all data that has been actually written. If `false` is specified, only an object containing the written count is returned like: `{"count": 42}`
 * @param {number} version = null The version number to be used for the change. That must be a version larger than the current HEAD version or the current HEAD version. If not specified, HEAD + 1 will be used by default
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_features(json_input TEXT, input_type TEXT, author TEXT, return_result BOOLEAN = false, version BIGINT = NULL,
    --The following parameters are not necessary for input_type = "Modifications"
    on_exists TEXT = NULL, on_not_exists TEXT = NULL, on_version_conflict TEXT = NULL, on_merge_conflict TEXT = NULL, is_partial BOOLEAN = false)
    RETURNS TEXT AS
$BODY$
    ${{Exception.js}}
    try {
      //Actual executions
      if (json_input == null)
        throw new Error("Parameter json_input must not be null.");

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

      // load required js libs
      // plv8.execute("SELECT require( 'jsonpath_rfc9535' )");

      //Init block of internal feature_writer functionality
      ${{FeatureWriter.js}}
      ${{DatabaseWriter.js}}
      //Init completed

      let input = JSON.parse(json_input);

      if (input_type == "FeatureCollection") {
        input = input.features;
        input_type = "Features";
      }

      let result;
      if (input_type == "Modifications")
        result = FeatureWriter.writeFeatureModifications(input, author, version == null ? undefined : version);
      else if (input_type == "Features")
        result = FeatureWriter.writeFeatures(input, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial, null, version == null ? undefined : version);
      else if (input_type == "Feature")
        result = FeatureWriter.writeFeature(input, author, on_exists, on_not_exists, on_version_conflict, on_merge_conflict, is_partial, null, version == null ? undefined : version);
      else
        throw new Error("Invalid input_type: " + input_type);

      return JSON.stringify(return_result ? result : {count: result.features.length, conflicting: result.conflicting.length});
    }
    catch (error) {
      if (!error.code)
        throw new Exception("Unexpected error in feature_writer: " + error.message, error);
      else
        throw error;
    }
$BODY$ LANGUAGE plv8 VOLATILE;