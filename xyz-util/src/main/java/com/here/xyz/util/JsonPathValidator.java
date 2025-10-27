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

package com.here.xyz.util;

/*
 * Usage:
 *   JsonPathValidator.ValidationResult r =
 *       JsonPathValidator.validate("$.store.book[?(@.price < 10)].title");
 *   if (!r.isValid()) {
 *       System.err.println(r.errorWithPointer(input));
 *   }
 */

import java.util.*;

public final class JsonPathValidator {
    private static final boolean REQUIRE_STRING_FOR_REGEX = true;

    public static ValidationResult validate(String input) {
        if (input == null)
            return ValidationResult.error(0, "Input is null");

        try {
            Lexer lexer = new Lexer(input);
            List<Token> tokens = lexer.lex();
            Parser parser = new Parser(tokens);
            parser.parsePath();
            parser.expect(TokenType.EOF, "Unexpected trailing");
            return ValidationResult.ok();
        } catch (ParseException pe) {
            return ValidationResult.error(pe.position, pe.getMessage());
        }
    }

    public static final class ValidationResult {
        private final boolean valid;
        private final int position;
        private final String message;

        private ValidationResult(boolean valid, int position, String message) {
            this.valid = valid;
            this.position = position;
            this.message = message;
        }

        public static ValidationResult ok() {
            return new ValidationResult(true, -1, null);
        }

        public static ValidationResult error(int pos, String msg) {
            return new ValidationResult(false, pos, msg);
        }

        public boolean isValid() {
            return valid;
        }

        public OptionalInt errorPosition() {
            return valid ? OptionalInt.empty() : OptionalInt.of(position);
        }

        public Optional<String> errorMessage() {
            return Optional.ofNullable(message);
        }

        public String errorWithPointer() {
            if (valid)
                return "OK";

            return (message == null ? "invalid JSONPath" : message) +
                    " at position " + position;
        }
    }

    private enum TokenType {
        // structural characters
        DOLLAR, DOT, STAR, LBRACKET, RBRACKET, LPAREN, RPAREN, COMMA, COLON, QUESTION, AT,
        // operators
        EQ, NE, LT, LE, GT, GE, AND, OR, NOT, REGEXMATCH,
        // literals
        NUMBER, STRING, TRUE, FALSE, NULL,
        // identifiers(unquoted names after dot)
        IDENT,
        EOF
    }

    private static final class Token {
        final TokenType type;
        final String text;
        final int position;

        Token(TokenType t, String text, int pos) {
            this.type = t;
            this.text = text;
            this.position = pos;
        }

        public String toString() {
            return type + (text != null ? ("(" + text + ")") : "") + "@" + position;
        }
    }

    private static final class Lexer {
        private final String str;
        private final int length;
        private int idx;

        Lexer(String s) {
            this.str = s;
            this.length = s.length();
        }

        List<Token> lex() {
            List<Token> out = new ArrayList<>();
            while (true) {
                skipWhitespace();

                if (idx >= length) {
                    out.add(new Token(TokenType.EOF, "", idx));
                    break;
                }

                char c = str.charAt(idx);
                int pos = idx;
                switch (c) {
                    case '$':
                        idx++;
                        out.add(new Token(TokenType.DOLLAR, "$", pos));
                        break;
                    case '.':
                        idx++;
                        out.add(new Token(TokenType.DOT, ".", pos));
                        break;
                    case '*':
                        idx++;
                        out.add(new Token(TokenType.STAR, "*", pos));
                        break;
                    case '[':
                        idx++;
                        out.add(new Token(TokenType.LBRACKET, "[", pos));
                        break;
                    case ']':
                        idx++;
                        out.add(new Token(TokenType.RBRACKET, "]", pos));
                        break;
                    case '(':
                        idx++;
                        out.add(new Token(TokenType.LPAREN, "(", pos));
                        break;
                    case ')':
                        idx++;
                        out.add(new Token(TokenType.RPAREN, ")", pos));
                        break;
                    case ',':
                        idx++;
                        out.add(new Token(TokenType.COMMA, ",", pos));
                        break;
                    case ':':
                        idx++;
                        out.add(new Token(TokenType.COLON, ":", pos));
                        break;
                    case '?':
                        idx++;
                        out.add(new Token(TokenType.QUESTION, "?", pos));
                        break;
                    case '@':
                        idx++;
                        out.add(new Token(TokenType.AT, "@", pos));
                        break;
                    case '!':
                        idx++;
                        if (match('='))
                            out.add(new Token(TokenType.NE, "!=", pos));
                        else
                            out.add(new Token(TokenType.NOT, "!", pos));
                        break;
                    case '=':
                        idx++;
                        if (match('='))
                            out.add(new Token(TokenType.EQ, "==", pos));
                        else if (match('~'))
                            out.add(new Token(TokenType.REGEXMATCH, "=~", pos));
                        else
                            throw err(pos, "expected '=' or '~' after '='");
                        break;
                    case '<':
                        idx++;
                        if (match('='))
                            out.add(new Token(TokenType.LE, "<=", pos));
                        else
                            out.add(new Token(TokenType.LT, "<", pos));
                        break;
                    case '>':
                        idx++;
                        if (match('='))
                            out.add(new Token(TokenType.GE, ">=", pos));
                        else
                            out.add(new Token(TokenType.GT, ">", pos));
                        break;
                    case '\'':
                        out.add(readString('\'', pos));
                        break;
                    case '"':
                        out.add(readString('"', pos));
                        break;
                    case '&':
                        idx++;
                        if (match('&'))
                            out.add(new Token(TokenType.AND, "&&", pos));
                        else
                            throw err(pos, "single '&' not allowed");
                        break;
                    case '|':
                        idx++;
                        if (match('|'))
                            out.add(new Token(TokenType.OR, "||", pos));
                        else
                            throw err(pos, "single '|' not allowed");
                        break;
                    default:
                        if (isDigit(c) || (c == '-' && (idx + 1 < length) && isDigit(str.charAt(idx + 1)))) {
                            out.add(readNumber(pos));
                        } else if (isIdentStart(c)) {
                            String ident = readIdent();
                            TokenType kw = keyword(ident);
                            out.add(new Token(kw == null ? TokenType.IDENT : kw, ident, pos));
                        } else {
                            throw err(pos, "unexpected character '" + c + "'");
                        }
                }
            }
            return out;
        }

        private void skipWhitespace() {
            while (idx < length) {
                char c = str.charAt(idx);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
                    idx++;
                else
                    break;
            }
        }

        private boolean match(char ch) {
            if (idx < length && str.charAt(idx) == ch) {
                idx++;
                return true;
            }
            return false;
        }

        private Token readString(char quote, int startPos) {
            StringBuilder sb = new StringBuilder();
            idx++; // consume opening quote
            boolean escaped = false;
            while (idx < length) {
                char c = str.charAt(idx++);
                if (escaped) {
                    switch (c) {
                        case '"':
                        case '\'':
                        case '\\':
                        case '/':
                            sb.append(c);
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'u':
                            if (idx + 4 > length)
                                throw err(startPos, "unterminated unicode escape");
                            String hex = str.substring(idx, idx + 4);
                            if (!hex.matches("[0-9A-Fa-f]{4}"))
                                throw err(startPos, "invalid unicode escape");
                            sb.append((char) Integer.parseInt(hex, 16));
                            idx += 4;
                            break;
                        default:
                            throw err(startPos, "invalid escape \\" + c);
                    }
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == quote) {
                    return new Token(TokenType.STRING, sb.toString(), startPos);
                } else {
                    sb.append(c);
                }
            }
            throw err(startPos, "unterminated string literal");
        }

        private Token readNumber(int startPos) {
            int j = idx;
            if (str.charAt(j) == '-')
                j++;

            if (j >= length || !isDigit(str.charAt(j)))
                throw err(startPos, "invalid number");

            if (str.charAt(j) == '0') {
                j++;
            } else {
                while (j < length && isDigit(str.charAt(j)))
                    j++;
            }

            if (j < length && str.charAt(j) == '.') {
                j++;
                if (j >= length || !isDigit(str.charAt(j)))
                    throw err(startPos, "invalid fraction");
                while (j < length && isDigit(str.charAt(j)))
                    j++;
            }

            if (j < length && (str.charAt(j) == 'e' || str.charAt(j) == 'E')) {
                j++;
                if (j < length && (str.charAt(j) == '+' || str.charAt(j) == '-'))
                    j++;
                if (j >= length || !isDigit(str.charAt(j)))
                    throw err(startPos, "invalid exponent");
                while (j < length && isDigit(str.charAt(j)))
                    j++;
            }

            String num = str.substring(idx, j);
            idx = j;
            return new Token(TokenType.NUMBER, num, startPos);
        }

        private String readIdent() {
            int j = idx;
            j++;
            while (j < length && isIdentPart(str.charAt(j)))
                j++;

            String id = str.substring(idx, j);
            idx = j;
            return id;
        }

        private static boolean isDigit(char c) {
            return c >= '0' && c <= '9';
        }

        private static boolean isIdentStart(char c) {
            return (c == '_' || Character.isLetter(c));
        }

        private static boolean isIdentPart(char c) {
            return (c == '_' || Character.isLetterOrDigit(c));
        }

        private static TokenType keyword(String ident) {
            if ("true".equals(ident))
                return TokenType.TRUE;
            if ("false".equals(ident))
                return TokenType.FALSE;
            if ("null".equals(ident))
                return TokenType.NULL;

            return null;
        }

        private static ParseException err(int pos, String msg) {
            return new ParseException(pos, msg);
        }
    }

    private static final class Parser {
        private final List<Token> tokens;
        private int idx = 0;

        Parser(List<Token> tokens) {
            this.tokens = tokens;
        }

        void parsePath() {
            expect(TokenType.DOLLAR, "path must start with '$'");
            while (!peek(TokenType.EOF)) {
                if (accept(TokenType.DOT)) {
                    if (accept(TokenType.STAR)) {
                        // $.* (wildcard member)
                    } else if (peek(TokenType.STRING)) {
                        // $."quoted"
                        next();
                    } else {
                        // $.name(unquoted identifier)
                        expect(TokenType.IDENT, "expected member name after '.'");
                    }
                } else if (accept(TokenType.LBRACKET)) {
                    if (accept(TokenType.QUESTION)) { // filter
                        expect(TokenType.LPAREN, "expected '(' after '?'");
                        parseBooleanExpr();
                        expect(TokenType.RPAREN, "expected ')' to close filter expression");
                        expect(TokenType.RBRACKET, "expected ']' to close filter");
                    } else if (accept(TokenType.STAR)) { // [*]
                        expect(TokenType.RBRACKET, "expected ']' after '*'");
                    } else if (peek(TokenType.STRING)) { // ['name'] or ["name"] or unions
                        parseUnionOrMember();
                        expect(TokenType.RBRACKET, "expected ']' after bracket member/union");
                    } else if (peek(TokenType.NUMBER) || peek(TokenType.COLON)) { // index/slice/union starting with number (negatives included)
                        parseIndexSliceOrUnion();
                        expect(TokenType.RBRACKET, "expected ']' after array selector");
                    } else if (accept(TokenType.RBRACKET)) {
                        throw err(prev().position, "empty bracket selector '[]' is not allowed");
                    } else {
                        throw err(curr().position, "unexpected token in bracket selector: " + curr().type);
                    }
                } else {
                    break;
                }
            }
        }

        // Parses ['name'] or ["name"] or union of strings/numbers/* separated by commas
        private void parseUnionOrMember() {
            next();
            while (accept(TokenType.COMMA)) {
                if (accept(TokenType.STAR))
                    continue;
                if (peek(TokenType.STRING) || peek(TokenType.NUMBER)) {
                    next();
                } else
                    throw err(curr().position, "expected string/number/* in union");
            }
        }

        // Parses [index], [start:end[:step]], or unions like [0,1,2]
        private void parseIndexSliceOrUnion() {
            if (accept(TokenType.COLON)) {
                if (peek(TokenType.NUMBER))
                    next();
                if (accept(TokenType.COLON)) {
                    Token step = expect(TokenType.NUMBER, "expected slice step after second ':'");
                    if ("0".equals(step.text))
                        throw err(step.position, "slice step cannot be 0");
                }
                return;
            }

            parseNumericOrWildcard();
            if (accept(TokenType.COLON)) { // slice
                if (peek(TokenType.NUMBER))
                    next();
                if (accept(TokenType.COLON)) {
                    Token step = expect(TokenType.NUMBER, "expected slice step after second ':'");
                    if ("0".equals(step.text))
                        throw err(step.position, "slice step cannot be 0");
                }
                return;
            }

            // possible union continuation
            while (accept(TokenType.COMMA)) {
                parseNumericOrWildcardOrString();
            }
        }

        private void parseNumericOrWildcard() {
            if (accept(TokenType.STAR))
                return;
            expect(TokenType.NUMBER, "expected number");
        }

        private void parseNumericOrWildcardOrString() {
            if (accept(TokenType.STAR))
                return;
            if (accept(TokenType.STRING))
                return;
            expect(TokenType.NUMBER, "expected number");
        }

        // Boolean expression for filters (precedence: ! > && > ||)
        private void parseBooleanExpr() {
            parseOr();
        }

        private void parseOr() {
            parseAnd();
            while (accept(TokenType.OR)) {
                parseAnd();
            }
        }

        private void parseAnd() {
            parseNot();
            while (accept(TokenType.AND)) {
                parseNot();
            }
        }

        private void parseNot() {
            while (accept(TokenType.NOT)) {
            }
            parsePrimaryBool();
        }

        private void parsePrimaryBool() {
            if (accept(TokenType.LPAREN)) {
                parseBooleanExpr();
                expect(TokenType.RPAREN, "expected ')' in expression");
                return;
            }

            // Try to consume left value, either @-relative path or a literal
            boolean leftConsumed = false;
            if (peek(TokenType.AT)) {
                parseRelativePath();
                leftConsumed = true;
            } else if (peek(TokenType.STRING) || peek(TokenType.NUMBER) ||
                    peek(TokenType.TRUE) || peek(TokenType.FALSE) || peek(TokenType.NULL)) {
                next(); // consume the literal
                leftConsumed = true;
            }

            if (leftConsumed) {
                // Enables both comparisons and existence predicates like ?(@.a))
                boolean isRegex = false;
                if (accept(TokenType.EQ) || accept(TokenType.NE) || accept(TokenType.LT) || accept(TokenType.LE) ||
                        accept(TokenType.GT) || accept(TokenType.GE) || (isRegex = accept(TokenType.REGEXMATCH))) {
                    if (peek(TokenType.AT)) {
                        if (isRegex && REQUIRE_STRING_FOR_REGEX)
                            throw err(curr().position, "right operand of '=~' must be a string");
                        parseRelativePath();
                    } else if (peek(TokenType.STRING) || peek(TokenType.NUMBER) ||
                            peek(TokenType.TRUE) || peek(TokenType.FALSE) || peek(TokenType.NULL)) {
                        if (isRegex && REQUIRE_STRING_FOR_REGEX && !peek(TokenType.STRING))
                            throw err(curr().position, "right operand of '=~' must be a string");
                        next();
                    } else {
                        throw err(curr().position, "expected value or @-path after operator");
                    }
                }

                return;
            }

            throw err(curr().position, "expected boolean expression");
        }

        private void parseRelativePath() {
            expect(TokenType.AT, "expected '@' for relative path");

            while (true) {
                if (accept(TokenType.DOT)) {
                    if (accept(TokenType.STAR)) {
                        // @.* (wildcard)
                    } else if (accept(TokenType.STRING)) {
                        // @."quoted"
                    } else {
                        expect(TokenType.IDENT, "expected member name after '.'");
                    }
                } else if (accept(TokenType.LBRACKET)) {
                    if (accept(TokenType.STAR)) {
                        expect(TokenType.RBRACKET, "expected ']' after '*'");
                    } else if (peek(TokenType.STRING)) {
                        parseUnionOrMember();
                        expect(TokenType.RBRACKET, "expected ']' after bracket member");
                    } else if (peek(TokenType.NUMBER) || peek(TokenType.COLON)) {
                        parseIndexSliceOrUnion();
                        expect(TokenType.RBRACKET, "expected ']' after array selector");
                    } else {
                        throw err(curr().position, "unexpected token in relative bracket selector");
                    }
                } else {
                    break;
                }
            }
        }

        private boolean accept(TokenType t) {
            if (peek(t)) {
                idx++;
                return true;
            }
            return false;
        }

        private Token expect(TokenType t, String msg) {
            if (!peek(t))
                throw err(curr().position, msg);
            return next();
        }

        private boolean peek(TokenType t) {
            return tokens.get(idx).type == t;
        }

        private Token next() {
            return tokens.get(idx++);
        }

        private Token curr() {
            return tokens.get(idx);
        }

        private Token prev() {
            return tokens.get(idx - 1);
        }

        private static ParseException err(int pos, String msg) {
            return new ParseException(pos, msg);
        }
    }

    private static final class ParseException extends RuntimeException {
        final int position;

        ParseException(int position, String message) {
            super(message);
            this.position = position;
        }
    }
}
