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

package com.here.xyz.test;

import com.here.xyz.util.JsonPathValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonPathValidatorTest {

    private void assertValid(String expr) {
        var res = JsonPathValidator.validate(expr);
        assertTrue(res.isValid(), () -> "Expected valid, got error: " + res.errorMessage().orElse("") +
                " at " + res.errorPosition().orElse(-1) + " for expr: " + expr);
    }

    private void assertInvalid(String expr) {
        var res = JsonPathValidator.validate(expr);
        assertFalse(res.isValid(), () -> "Expected invalid but was valid: " + expr);
    }

    @Test
    @DisplayName("Must start with $")
    void mustStartWithDollar() {
        assertInvalid("a.b");
        assertValid("$.a");
    }

    @Test
    @DisplayName("Only root($)")
    void rootOnly() {
        assertValid("$");
    }

    @Test
    @DisplayName("Simple dot and quoted members")
    void dotAndQuotedMembers() {
        assertValid("$.a");
        assertValid("$.a.b");
        assertValid("$.\"spaced name\"");
        assertValid("$['spaced name']");
        assertValid("$['member\\'quote']");
    }

    @Test
    @DisplayName("Wildcards and bracket members")
    void wildcardsAndBracketMembers() {
        assertValid("$.*");
        assertValid("$['*']");
        assertValid("$[*]");
        assertValid("$.a[*]");
    }

    @Test
    @DisplayName("Array indices, slices, and unions")
    void indicesSlicesUnions() {
        assertValid("$.a[0]");
        assertValid("$.a[-1]");
        assertValid("$.a[0:10]");
        assertValid("$.a[:10]");
        assertValid("$.a[1:10:2]");
        assertValid("$.a[0,1,2]");
        assertValid("$.a[0,'x',\"y\",*]");
        assertValid("$.a[:]");
        assertValid("$.a[::2]");
        assertValid("$.a[0:]");
        assertValid("$.a[:10:2]");
        assertInvalid("$.a[0::]"); // step missing

        var r = JsonPathValidator.validate("$.a[1:4:0]"); // step cannot be 0
        assertFalse(r.isValid());
        assertTrue(r.errorMessage().orElse("").toLowerCase().contains("step"));
    }

    @Test
    @DisplayName("Reject recursive descent operator")
    void rejectRecursiveDescent() {
        assertInvalid("$.a..b");
    }

    @Test
    @DisplayName("Filters: literals, comparisons, existence, grouping")
    void filters() {
        // comparisons and literals
        assertValid("$.store.book[?(@.price >= 0)]");
        assertValid("$.store.book[?(@.author == 'John')]");
        assertValid("$.store.book[?(@.author != 'John')]");
        assertValid("$.store.book[?(!(@.soldOut))]");
        // existence
        assertValid("$.items[?(@.name)]");
        // regex
        assertValid("$.items[?(@.name =~ '^[A-Z].*')]");
        // invalid single '='
        assertInvalid("$.a[?(@.x = 1)]");
    }

    @Test
    @DisplayName("Comparison right-hand side missing")
    void comparisonRhsMissing() {
        assertInvalid("$.a[?(@.x == )]");
        assertInvalid("$.a[?(@.x >= )]");
    }

    @Test
    @DisplayName("Unterminated braces and parans")
    void unterminated() {
        assertInvalid("$.a[");
        assertInvalid("$.a[?( @.x == 1 )");
        assertInvalid("$.a[?(@.x == (1)]");
    }

    @Test
    @DisplayName("Relative wildcards are allowed")
    void relativeWildcard() {
        assertValid("$.a[?(@.*)]");
        assertValid("$.a[?(@[*])]");
    }

    @Test
    @DisplayName("Regex must be string")
    void regexRightMustBeString() {
        assertInvalid("$.items[?(@.name =~ 123)]");
    }

    @Test
    @DisplayName("Unicode strings in bracket notation")
    void unicodeStrings() {
        assertValid("$['café']");
        assertValid("$['\u00E9']");
        assertInvalid("$['\\u00GZ']");
    }

    @Test
    @DisplayName("Empty or malformed bracket selectors")
    void emptyMalformedBrackets() {
        assertInvalid("$[]");
        assertInvalid("$.a[,,]");
    }

    @Test
    @DisplayName("Logical operators")
    void logicalOperators() {
        assertValid("$.a[?(@.x && @.y)]");
        assertValid("$.a[?(!@.x || @.y)]");
        assertInvalid("$.a[?(@.x & @.y)]");
        assertInvalid("$.a[?(@.x | @.y)]");
    }

    @Nested
    @DisplayName("Relative @-paths inside filters")
    class RelativePaths {

        @Test
        void simple() {
            assertValid("$.a[?(@.b)]");
        }
        @Test void withIndex() {
            assertValid("$.a[?(@[0])]");
        }
        @Test void withBracketMembers() {
            assertValid("$.a[?(@['x'])]");
        }
        @Test void withSlice() {
            assertValid("$.a[?(@[1:3])]");
        }
    }

    @Test
    @DisplayName("Null, true, false keywords allowed in filters")
    void primitives() {
        assertValid("$.a[?(true)]");
        assertValid("$.a[?(false)]");
        assertValid("$.a[?(null)]");
        assertValid("$.a[?(@.a == null)]");
    }

    @Test
    @DisplayName("Unexpected trailing input should fail")
    void trailingInput() {
        assertInvalid("$.a]extra");
        assertInvalid("$.a)extra");
    }

    @Test
    @DisplayName("Empty input is not allowed")
    void emptyInput() {
        assertInvalid("");
    }

    @Test
    @DisplayName("Trailing Whitespace is allowed")
    void trailingWhitespace() {
        assertValid("$.a  ");
    }
}

