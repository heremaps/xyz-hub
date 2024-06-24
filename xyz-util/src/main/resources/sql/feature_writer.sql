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
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_features(inputFeatures TEXT, author TEXT, onExists TEXT,
    onNotExists TEXT, onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_feature(inputFeature JSONB, baseVersion TEXT, onExists TEXT,
    onNotExists TEXT, onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError
 */
CREATE OR REPLACE FUNCTION delete_feature(inputFeature JSONB, baseVersion TEXT,
    onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError, FeatureExistsError
 */
CREATE OR REPLACE FUNCTION write_row(inputFeature JSONB, baseVersion TEXT, onExists TEXT,
    onNotExists TEXT, onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError
 */
CREATE OR REPLACE FUNCTION delete_row(inputFeature JSONB, baseVersion TEXT, onVersionConflict TEXT)
    RETURNS VOID AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws VersionConflictError, MergeConflictError
 */
CREATE OR REPLACE FUNCTION handle_version_conflict(inputFeature JSONB, baseVersion TEXT,
    onVersionConflict TEXT, onMergeConflict TEXT, isPartial TEXT, headFeature JSONB)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws MergeConflictError
 */
CREATE OR REPLACE FUNCTION handle_merge_conflict(inputFeature JSONB, baseVersion TEXT,
    onMergeConflict TEXT, isPartial TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

CREATE OR REPLACE FUNCTION load_feature(id TEXT, version BIGINT = -1)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

CREATE OR REPLACE FUNCTION diff(minuend JSONB, subtrahend JSONB) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

CREATE OR REPLACE FUNCTION patch(target JSONB, inputDiff JSONB) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

CREATE OR REPLACE FUNCTION enrich_feature(inputFeature JSONB, version BIGINT, author TEXT)
    RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;

/**
 * @throws MergeConflictError
 */
CREATE OR REPLACE FUNCTION merge_changes(headFeature JSONB, inputDiff JSONB, headDiff JSONB,
    baseVersion BIGINT, onMergeConflict TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE;