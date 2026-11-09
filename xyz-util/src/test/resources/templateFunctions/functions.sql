/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
-- The two functions of ext.sql (xyz-psql-connector) that the creation of a space table calls. The installer tests of the
-- empty-space template use this script instead of ext.sql, which is not on the class path of xyz-util. The functions exist only
-- in the script schema, so a template installation succeeds only with the script schemas on its search path.
CREATE OR REPLACE FUNCTION max_bigint() RETURNS BIGINT AS
$BODY$
BEGIN
    RETURN 9223372036854775807::BIGINT;
END
$BODY$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION xyz_create_history_partition(schema TEXT, rootTable TEXT, partitionNo BIGINT, partitionSize BIGINT)
    RETURNS VOID AS
$BODY$
BEGIN
    EXECUTE
        format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
            schema, (rootTable || '_p' || partitionNo), schema, rootTable,
            partitionSize * partitionNo, partitionSize * (partitionNo + 1));
END
$BODY$
LANGUAGE plpgsql VOLATILE;
