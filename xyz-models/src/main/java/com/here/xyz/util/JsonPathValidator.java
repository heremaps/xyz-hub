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

package com.here.xyz.util;

import org.noear.snack4.jsonpath.*;

import java.util.ArrayList;
import java.util.List;

public final class JsonPathValidator {

    private JsonPathValidator() {
    }

    public static boolean isValid(String jsonPath) {
        if (jsonPath == null)
            return false;

        final String p = jsonPath.trim();
        if (p.isEmpty())
            return false;

        final char first = p.charAt(0);
        if (first != '$' && first != '@')
            return false;

        // Structural checks (fixes snack4json open parsing such as "$[" or "$['a]")
        if (!validateTopLevelQueryStructure(p))
            return false;

        // Runtime compatibility with snack4json
        try {
            JsonPath.parse(p);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private static boolean validateTopLevelQueryStructure(String p) {
        int i = 1;
        final int n = p.length();

        while (true) {
            i = skipBlanks(p, i);
            if (i >= n)
                return true;

            final char ch = p.charAt(i);

            if (ch == '[') {
                i = parseBracketSegmentAndValidate(p, i);
                if (i < 0)
                    return false;
                continue;
            }

            if (ch == '.') {
                if (i + 1 >= n)
                    return false;

                // descendant ".."
                if (p.charAt(i + 1) == '.') {
                    i += 2;
                    if (i >= n)
                        return false;
                    // No blanks allowed immediately after ".." for shorthand forms
                    if (isBlank(p.charAt(i)))
                        return false;

                    final char next = p.charAt(i);
                    if (next == '*') {
                        i++;
                        continue;
                    }
                    if (next == '[') {
                        i = parseBracketSegmentAndValidate(p, i);
                        if (i < 0)
                            return false;
                        continue;
                    }

                    final int j = parseMemberNameShorthand(p, i);
                    if (j < 0)
                        return false;

                    i = j;
                    continue;
                }

                // child "."
                i += 1;
                if (i >= n)
                    return false;
                if (isBlank(p.charAt(i)))
                    return false;

                final char next = p.charAt(i);
                if (next == '*') {
                    i++;
                    continue;
                }

                if (next == '[')
                    return false;

                final int j = parseMemberNameShorthand(p, i);
                if (j < 0)
                    return false;

                i = j;
                continue;
            }

            return false;
        }
    }

    private static int skipBlanks(String s, int i) {
        final int n = s.length();
        while (i < n && isBlank(s.charAt(i)))
            i++;

        return i;
    }

    private static boolean isBlank(char c) {
        return Character.isWhitespace(c);
    }

    private static int parseMemberNameShorthand(String s, int i) {
        final int n = s.length();
        if (i >= n)
            return -1;

        final int cp0 = s.codePointAt(i);
        if (!isNameFirst(cp0))
            return -1;

        i += Character.charCount(cp0);

        while (i < n) {
            final int cp = s.codePointAt(i);
            if (!isNameChar(cp))
                break;

            i += Character.charCount(cp);
        }

        return i;
    }

    private static boolean isNameFirst(int cp) {
        if (cp == '_')
            return true;
        if (cp >= 'A' && cp <= 'Z')
            return true;
        if (cp >= 'a' && cp <= 'z')
            return true;

        return (cp >= 0x80 && cp <= 0xD7FF) || (cp >= 0xE000 && cp <= 0x10FFFF);
    }

    private static boolean isNameChar(int cp) {
        return isNameFirst(cp) || (cp >= '0' && cp <= '9');
    }

    private static int parseBracketSegmentAndValidate(String p, int openIdx) {
        final int closeIdx = findMatchingBracket(p, openIdx);
        if (closeIdx < 0)
            return -1;

        final String inside = p.substring(openIdx + 1, closeIdx);
        if (!validateBracketedSelection(inside))
            return -1;

        return closeIdx + 1;
    }

    private static int findMatchingBracket(String s, int openIdx) {
        final int n = s.length();
        if (openIdx >= n || s.charAt(openIdx) != '[')
            return -1;

        boolean inSingle = false, inDouble = false, inRegex = false;
        int depth = 0;

        for (int i = openIdx; i < n; i++) {
            final char c = s.charAt(i);

            if (inSingle) {
                if (c == '\\') {
                    if (++i >= n)
                        return -1;
                    continue;
                }

                if (c == '\'')
                    inSingle = false;
                continue;
            }
            if (inDouble) {
                if (c == '\\') {
                    if (++i >= n)
                        return -1;
                    continue;
                }

                if (c == '"')
                    inDouble = false;
                continue;
            }
            if (inRegex) {
                if (c == '\\') {
                    if (++i >= n)
                        return -1;
                    continue;
                }

                if (c == '/')
                    inRegex = false;
                continue;
            }

            if (c == '\'') {
                inSingle = true;
                continue;
            }

            if (c == '"') {
                inDouble = true;
                continue;
            }

            if (c == '/') {
                inRegex = true;
                continue;
            }

            if (c == '[') {
                depth++;
                continue;
            }

            if (c == ']') {
                depth--;
                if (depth == 0)
                    return i;
                if (depth < 0)
                    return -1;
            }
        }

        return -1;
    }

    // Bracketed-selection = "[" S selector *(S "," S selector) S "]"
    private static boolean validateBracketedSelection(String inside) {
        if (inside == null)
            return false;

        final String trimmed = inside.trim();
        if (trimmed.isEmpty())
            return false; // "$[]" or "$[ ]" invalid

        final List<String> selectors = splitSelectorsStrict(trimmed);
        if (selectors == null || selectors.isEmpty())
            return false;

        for (String sel : selectors) {
            if (!validateSelector(sel))
                return false;
        }

        return true;
    }

    private static List<String> splitSelectorsStrict(String s) {
        boolean inSingle = false, inDouble = false, inRegex = false;
        int bracket = 0, paren = 0, brace = 0;

        final List<String> out = new ArrayList<>();
        final StringBuilder sb = new StringBuilder();

        for (int i = 0, n = s.length(); i < n; i++) {
            final char c = s.charAt(i);

            if (inSingle) {
                sb.append(c);
                if (c == '\\') {
                    if (++i >= n)
                        return null;

                    sb.append(s.charAt(i));
                    continue;
                }
                if (c == '\'')
                    inSingle = false;
                continue;
            }
            if (inDouble) {
                sb.append(c);
                if (c == '\\') {
                    if (++i >= n)
                        return null;

                    sb.append(s.charAt(i));
                    continue;
                }
                if (c == '"')
                    inDouble = false;
                continue;
            }
            if (inRegex) {
                sb.append(c);
                if (c == '\\') {
                    if (++i >= n)
                        return null;

                    sb.append(s.charAt(i));
                    continue;
                }
                if (c == '/')
                    inRegex = false;
                continue;
            }

            switch (c) {
                case '\'' -> {
                    inSingle = true;
                    sb.append(c);
                }
                case '"' -> {
                    inDouble = true;
                    sb.append(c);
                }
                case '/' -> {
                    inRegex = true;
                    sb.append(c);
                }
                case '[' -> {
                    bracket++;
                    sb.append(c);
                }
                case ']' -> {
                    bracket--;
                    if (bracket < 0)
                        return null;
                    sb.append(c);
                }
                case '(' -> {
                    paren++;
                    sb.append(c);
                }
                case ')' -> {
                    paren--;
                    if (paren < 0)
                        return null;
                    sb.append(c);
                }
                case '{' -> {
                    brace++;
                    sb.append(c);
                }
                case '}' -> {
                    brace--;
                    if (brace < 0)
                        return null;
                    sb.append(c);
                }
                case ',' -> {
                    if (bracket == 0 && paren == 0 && brace == 0) {
                        final String chunk = sb.toString().trim();
                        if (chunk.isEmpty())
                            return null;
                        out.add(chunk);
                        sb.setLength(0);
                    } else {
                        sb.append(c);
                    }
                }
                default -> sb.append(c);
            }
        }

        if (inSingle || inDouble || inRegex)
            return null;

        if (bracket != 0 || paren != 0 || brace != 0)
            return null;

        final String last = sb.toString().trim();
        if (last.isEmpty())
            return null;

        out.add(last);
        return out;
    }

    private static boolean validateSelector(String sel) {
        final String s = sel.trim();
        if (s.isEmpty())
            return false;

        if (s.equals("*"))
            return true;

        if (s.startsWith("'") || s.startsWith("\"")) {
            return isValidStringLiteral(s);
        }

        if (s.startsWith("?")) {
            String expr = s.substring(1).trim();
            return isFilterExpressionPlausiblyComplete(expr);
        }

        if (s.indexOf(':') >= 0) {
            return isValidSliceSelector(s);
        }

        return isValidIntToken(s) && !s.equals("-0");
    }

    private static boolean isFilterExpressionPlausiblyComplete(String expr) {
        if (expr == null)
            return false;

        String e = expr.trim();
        if (e.isEmpty())
            return false;

        while (true) {
            int end = e.length() - 1;
            while (end >= 0 && Character.isWhitespace(e.charAt(end)))
                end--;

            if (end < 0)
                return false;

            if (e.charAt(end) == ')') {
                e = e.substring(0, end).trim();
                if (e.isEmpty())
                    return false;

                continue;
            }
            break;
        }

        String[] trailingOps = {
                "==", "!=", "<=", ">=", "&&", "||", "<", ">", "=~"
        };

        for (String op : trailingOps) {
            if (e.endsWith(op))
                return false;
        }

        char last = e.charAt(e.length() - 1);
        if (last == '=' || last == '&' || last == '|' || last == '!' || last == ',' || last == '(') {
            return false;
        }

        return true;
    }

    private static boolean isValidSliceSelector(String s) {
        final String[] parts = s.split(":", -1);
        if (parts.length < 2 || parts.length > 3)
            return false;

        for (int i = 0; i < parts.length; i++) {
            final String p = parts[i].trim();
            if (p.isEmpty())
                continue;

            if (!isValidIntToken(p))
                return false;

            if (p.equals("-0"))
                return false;
        }

        return true;
    }

    private static boolean isValidIntToken(String s) {
        if (s.equals("0"))
            return true;

        if (s.startsWith("-")) {
            final String rest = s.substring(1);
            return isValidNonZeroUnsignedInt(rest);
        }

        return isValidNonZeroUnsignedInt(s);
    }

    private static boolean isValidNonZeroUnsignedInt(String s) {
        if (s.isEmpty())
            return false;

        if (s.charAt(0) < '1' || s.charAt(0) > '9')
            return false;

        for (int i = 1; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c < '0' || c > '9')
                return false;
        }

        return true;
    }

    private static boolean isValidStringLiteral(String s) {
        if (s.length() < 2)
            return false;

        final char quote = s.charAt(0);
        if (quote != '\'' && quote != '"')
            return false;

        if (s.charAt(s.length() - 1) != quote)
            return false;

        for (int i = 1; i < s.length() - 1; i++) {
            final char c = s.charAt(i);

            // No unescaped control characters
            if (c <= 0x1F)
                return false;

            if (c == quote)
                return false;

            if (c == '\\') {
                if (i + 1 >= s.length() - 1)
                    return false;

                final char e = s.charAt(++i);
                switch (e) {
                    case 'b', 't', 'n', 'f', 'r', '"', '\'', '/', '\\' -> {
                    }
                    case 'u' -> {
                        if (i + 4 >= s.length() - 1)
                            return false;

                        for (int k = 0; k < 4; k++) {
                            final char h = s.charAt(i + 1 + k);
                            final boolean hex =
                                    (h >= '0' && h <= '9') ||
                                            (h >= 'a' && h <= 'f') ||
                                            (h >= 'A' && h <= 'F');

                            if (!hex)
                                return false;
                        }

                        i += 4;
                    }
                    default -> {
                        return false;
                    }
                }
            }
        }

        return true;
    }
}
