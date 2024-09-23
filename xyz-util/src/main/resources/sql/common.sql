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
 * Returns a context field's value.
 */
CREATE OR REPLACE FUNCTION context(key TEXT) RETURNS JSONB AS $BODY$
BEGIN
    RETURN (current_setting('xyz.queryContext')::JSONB)[key];
    EXCEPTION WHEN OTHERS THEN RETURN NULL;
END
$BODY$ LANGUAGE plpgsql VOLATILE;

/**
 * Sets the context (a static map) for the current query / transaction.
 */
CREATE OR REPLACE FUNCTION context(context JSONB) RETURNS VOID AS $BODY$
BEGIN
    PERFORM set_config('xyz.queryContext', context::TEXT, true);
END
$BODY$ LANGUAGE plpgsql VOLATILE;

/**
 * Returns the whole context object.
 */
CREATE OR REPLACE FUNCTION context() RETURNS JSONB AS $BODY$
BEGIN
    RETURN current_setting('xyz.queryContext')::JSONB;
    EXCEPTION WHEN OTHERS THEN RETURN '{}'::JSONB;
END
$BODY$ LANGUAGE plpgsql VOLATILE;

/**
 * Sets a context field to the specified value for the current query / transaction.
 */
CREATE OR REPLACE FUNCTION context(key TEXT, value ANYELEMENT) RETURNS VOID AS $BODY$
BEGIN
    --Inject / overwrite the field
    PERFORM context(jsonb_set(context(), ARRAY[key], to_jsonb(value), true));
END
$BODY$ LANGUAGE plpgsql VOLATILE;

/**
 * This function can be used to write logs, if we run in Async mode.
 */
CREATE OR REPLACE FUNCTION write_log(log_msg text, log_source text, log_level text DEFAULT 'INFO') RETURNS VOID AS
$BODY$
DECLARE
BEGIN
--     CREATE TABLE IF NOT EXISTS common."logs"
--     (
--         pid integer,
--         ts TIMESTAMP,
--         log text ,
--         level text ,
--         source text
--     );

    INSERT INTO common.logs(pid, ts, log , level , source)
				VALUES (pg_backend_pid(), NOW(), log_msg, log_source, log_level);
END
$BODY$
LANGUAGE plpgsql VOLATILE;