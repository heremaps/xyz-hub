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

package com.here.xyz.hub.util;

import com.here.xyz.models.hub.DataReference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.apache.commons.lang3.StringUtils.isBlank;

public final class DataReferenceValidator {

  private DataReferenceValidator() {
    // utility class
  }

  public static Collection<String> validateDataReference(DataReference dataReference) {
    List<String> violations = new ArrayList<>();

    violations.addAll(validateVersionsAndPatch(dataReference));
    violations.addAll(validateNotBlank(dataReference.getEntityId(), "entityId"));
    violations.addAll(validateNotBlank(dataReference.getObjectType(), "objectType"));
    violations.addAll(validateNotBlank(dataReference.getContentType(), "contentType"));
    violations.addAll(validateNotBlank(dataReference.getLocation(), "location"));
    violations.addAll(validateNotBlank(dataReference.getSourceSystem(), "sourceSystem"));
    violations.addAll(validateNotBlank(dataReference.getTargetSystem(), "targetSystem"));

    return violations;
  }

  private static Collection<String> validateVersionsAndPatch(DataReference dataReference) {
    List<String> violations = new ArrayList<>();

    Integer start = dataReference.getStartVersion();
    Integer end = dataReference.getEndVersion();
    boolean patch = dataReference.isPatch();

    boolean hasStart = start != null;
    boolean hasEnd = end != null;

    if (!hasEnd) {
      violations.add("Data Reference must contain an endVersion");
    }

    if (hasEnd && end < 0) {
      violations.add("Data Reference endVersion must be a non-negative integer");
    }

    if (hasEnd && hasStart && start >= end) {
      violations.add("Data Reference startVersion must be an integer less than endVersion");
    }

    if (hasStart && start < 0) {
      violations.add("Data Reference startVersion must be a non-negative integer");
    }

    if (patch && !hasStart) {
      violations.add("Data Reference startVersion must be set when isPatch is set to true");
    }

    if (!patch && hasStart) {
      violations.add("Data Reference startVersion must only be set when isPatch is set to true");
    }

    return violations;
  }

  private static Collection<String> validateNotBlank(String value, String attributeName) {
    return isBlank(value)
      ? Set.of("Data Reference %s must not be blank, empty or null".formatted(attributeName))
      : emptySet();
  }

}
