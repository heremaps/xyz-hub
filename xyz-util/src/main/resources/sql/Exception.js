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

class Exception extends Error {
  code = 0;
  context;
  detail;
  hint;
  cause;

  constructor(message, cause = null) {
    super(message);
    this.withDetail(this.constructor.name + ": ");
    this.cause = cause;

    //Build the context information
    this.context = this.stack;

    if (cause instanceof Exception)
      this.context += "\nCaused by: " + cause.context;
    else if (cause instanceof Error)
      this.context += "\nCaused by: " + cause.stack;
    else
      this.context += "\nCaused by: " + cause;
  }

  withCode(code) {
    //Valid characters are within the range of chars: "0" (ASCII: 49) - "o" (ASCII: 111), 00000 however will be mapped to "XX000"
    if (code == null || typeof code != "string" || code.length != 5)
      return this;

    let numericCode = 0;
    for (let i in code)
      numericCode += Math.pow(64, i) * (code[i].charCodeAt(0) - 48);
    this.code = numericCode;
    return this;
  }

  withDetail(detail) {
    this.detail = detail;
    return this;
  }

  withHint(hint) {
    this.hint = hint;
    return this;
  }
}

class XyzException extends Exception {
  constructor(message, cause = null) {
    super(message, cause);
    this.withCode("XYZ50");
  }
}

class VersionConflictError extends XyzException {
  constructor(message, cause = null) {
    super(message, cause);
    this.withCode("XYZ49");
  }
}

class MergeConflictError extends VersionConflictError {
  constructor(message, cause = null) {
    super(message, cause);
    this.withCode("XYZ48");
  }
}

class IllegalArgumentException extends XyzException {
  constructor(message, cause = null) {
    super(message, cause);
    this.withCode("XYZ40");
  }
}

class FeatureExistsException extends XyzException {
  constructor(message, cause = null) {
    super(message, cause);
    this.withCode("XYZ20");
  }
}

class FeatureNotExistsException extends XyzException {
  constructor(message, cause = null) {
    super(message, cause);
    this.withCode("XYZ44");
  }
}

SQLErrors = {
  CONFLICT: "23505"
};

if (plv8.global) {
  global.Exception = Exception;
  global.XyzException = XyzException;
  global.VersionConflictError = VersionConflictError;
  global.MergeConflictError = MergeConflictError;
  global.IllegalArgumentException = IllegalArgumentException;
  global.FeatureExistsException = FeatureExistsException;
  global.FeatureNotExistsException = FeatureNotExistsException;
  global.SQLErrors = SQLErrors;
}
