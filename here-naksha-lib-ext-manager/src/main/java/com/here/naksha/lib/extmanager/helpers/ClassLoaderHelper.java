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
package com.here.naksha.lib.extmanager.helpers;

import com.linkedin.cytodynamics.matcher.GlobMatcher;
import com.linkedin.cytodynamics.nucleus.DelegateRelationshipBuilder;
import com.linkedin.cytodynamics.nucleus.IsolationLevel;
import com.linkedin.cytodynamics.nucleus.LoaderBuilder;
import com.linkedin.cytodynamics.nucleus.OriginRestriction;
import java.io.File;
import java.util.Collections;
import java.util.List;

public class ClassLoaderHelper {

  /**
   * Load given jar using isolation class loader and return the class loader instance
   * @param jarFile File Instance of jar file
   * @return ClassLoader instance of ClassLoader
   */
  public static ClassLoader getClassLoader(File jarFile, List<String> whitelistedDelegatedClasses) {
    DelegateRelationshipBuilder delegateRelationshipBuilder =
        DelegateRelationshipBuilder.builder().withIsolationLevel(IsolationLevel.FULL);
    whitelistedDelegatedClasses.forEach(
        pkg -> delegateRelationshipBuilder.addDelegatePreferredClassPredicate(new GlobMatcher(pkg)));
    return LoaderBuilder.anIsolatingLoader()
        .withOriginRestriction(OriginRestriction.allowByDefault())
        .withClasspath(Collections.singletonList(jarFile.toURI()))
        .withParentRelationship(delegateRelationshipBuilder.build())
        .build();
  }
}
