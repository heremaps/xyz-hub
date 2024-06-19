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

-- The following methods are public and may be called by software directly

CREATE OR REPLACE FUNCTION write_feature(inputFeature JSONB, onVersionConflict TEXT, onMergeConflict TEXT, isPartial BOOLEAN) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION delete_feature(id TEXT, onVersionConflict TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

-- The following methods are internal to this module and may not be called by software directly

CREATE OR REPLACE FUNCTION handle_version_conflict(inputFeature JSONB, onVersionConflict TEXT, onMergeConflict TEXT, isPartial BOOLEAN) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION merge_changes(baseFeature JSONB, inputDiff JSONB, headDiff JSONB, onMergeConflict TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION handle_merge_conflict(inputDiff JSONB, headDiff JSONB, onMergeConflict TEXT) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION write_row(inputFeature JSONB, headFeature JSONB) RETURNS VOID AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION write_row(id TEXT, version BIGINT, operation CHAR, author TEXT, jsondata JSONB, geo GEOMETRY) RETURNS VOID AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

-- Some helper methods

CREATE OR REPLACE FUNCTION load_feature(id TEXT, version BIGINT = -1) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION diff(minuend JSONB, subtrahend JSONB) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION patch(target JSONB, inputDiff JSONB) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION has_version_conflict(inputFeature JSONB, headFeature JSONB) RETURNS BOOLEAN AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION enrich_feature(inputFeature JSONB) RETURNS JSONB AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION resolveOperation(inputFeature JSONB, headFeature JSONB) RETURNS CHAR AS $BODY$

$BODY$ LANGUAGE plv8 IMMUTABLE STRICT;