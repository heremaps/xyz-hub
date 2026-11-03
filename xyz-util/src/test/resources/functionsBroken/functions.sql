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
/* The second statement has a syntax error, so every installation of this script fails with a permanent error. */
CREATE OR REPLACE FUNCTION myTestFunction(input text) RETURNS TEXT AS
$BODY$
BEGIN
    RETURN 'Hello Broken, ' || input;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;
CREATE OR REPLACE FUNCTIN myBrokenFunction() RETURNS TEXT;