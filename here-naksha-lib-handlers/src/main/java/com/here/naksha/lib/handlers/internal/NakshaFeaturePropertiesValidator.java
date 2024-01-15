/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.handlers.internal;

import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

class NakshaFeaturePropertiesValidator {

  private NakshaFeaturePropertiesValidator() {}

  static Result nakshaFeatureValidation(NakshaFeature feature) {
    Result titleValidation = requiredPropertyValidationError(feature.getTitle(), NakshaFeature.TITLE);
    if (titleValidation instanceof ErrorResult) {
      return titleValidation;
    }
    Result descValidation = requiredPropertyValidationError(feature.getDescription(), NakshaFeature.DESCRIPTION);
    if (descValidation instanceof ErrorResult) {
      return descValidation;
    }
    return new SuccessResult();
  }

  private static @NotNull Result requiredPropertyValidationError(String value, String propertyName) {
    if (StringUtils.isBlank(value)) {
      return missingParameterError(propertyName);
    }
    return new SuccessResult();
  }

  private static ErrorResult missingParameterError(String propertyName) {
    return new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "Mandatory parameter '" + propertyName + "' missing!");
  }
}
