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

package com.here.xyz;

import com.here.xyz.util.JsonPathValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class JsonPathValidatorTest {

    @Test
    void nullAndBlankAreInvalid() {
        assertFalse(JsonPathValidator.isValid(null));
        assertFalse(JsonPathValidator.isValid(""));
        assertFalse(JsonPathValidator.isValid("   "));
        assertFalse(JsonPathValidator.isValid("\n\t\r"));
    }

    @Test
    void trimsInput() {
        assertTrue(JsonPathValidator.isValid("   $   "));
        assertTrue(JsonPathValidator.isValid(" \n $.o['j j'] \t "));
    }

    @ParameterizedTest(name = "valid: {0}")
    @MethodSource("validExamples")
    void validExamples(String p) {
        assertTrue(JsonPathValidator.isValid(p), p);
    }

    @ParameterizedTest(name = "invalid: {0}")
    @MethodSource("invalidExamples")
    void invalidExamples(String p) {
        assertFalse(JsonPathValidator.isValid(p), p);
    }

    static Stream<String> validExamples() {
        return Stream.of(
                "$",
                // name selectors (RFC examples)
                "$.o['j j']",
                "$.o['j j']['k.k']",
                "$.o[\"j j\"][\"k.k\"]",
                "$[\"'\"][\"@\"]",

                // wildcard selectors
                "$[*]",
                "$.o[*]",
                "$..*",
                "$..o[*]",

                // index + unions
                "$[0]",
                "$[0,1,2]",
                "$.a[0, 2, 4]",

                // slice
                "$[:]",
                "$[1:]",
                "$[:2]",
                "$[0:2]",
                "$[0:10:2]",
                "$[::2]",
                "$[-2:]",

                // descendant segments
                "$..o",
                "$..o['j j']",
                "$..o[0]",

                // filters (RFC examples)
                "$.a[?@.b == 'kilo']",
                "$.a[?(@.b == 'kilo')]",
                "$.a[?@>3.5]",
                "$.a[?@.b]",

                // functions inside filters (supported by snack4-jsonpath)
                "$.a[?length(@.b) > 0]"
        );
    }

    static Stream<String> invalidExamples() {
        return Stream.of(
                // must start with $ or @
                "a",
                ".a",
                "[]",
                " $a",
                "$a",

                // malformed dot segments, short-hands
                "$.",
                "$..",
                "$.  a",
                "$..  a",
                "$.123",
                "$.a-b",
                "$.[0]",

                // unterminated bracket or quote
                "$[",
                "$[]",
                "$[ ]",
                "$['a]",
                "$[\"a]",

                // comma/union
                "$[0,]",
                "$[,0]",
                "$[0,,1]",
                "$['a',]",
                "$[*,]",

                // int
                "$[01]",
                "$[-0]",

                // slice malformed
                "$[0:1:2:3]",
                "$[a:b]",

                // filter malformed
                "$[?]",
                "$.a[?@.b == ]",
                "$.a[?(@.b == 'kilo']",

                // invalid string escapes
                "$['\\u00G0']",
                "$[\"\\u12\"]"
        );
    }
}

