/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.auth;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * A matrix that describes the relationship between actions and attribute maps. This can either be used as access rights matrix or as
 * request matrix. When being used as access matrix the contained attribute maps represent the access rights the application or user does
 * have to resources. When being used as request matrix (while accessing resources) the attribute maps describe all access rights the
 * application or user need to have to perform the action.
 */
public class ActionMatrix extends LinkedHashMap<String, List<AttributeMap>> {

  /**
   * Adds the given attribute map to the provided action of this action matrix and returns this action matrix again. If no such action
   * exists a new action is created and the attributes map is added to a new list that is created. If the given attributes map or an equal
   * one is already part of the action, then the method will do nothing.
   *
   * @param action the name of the action.
   * @param attributesMap the attributes map to add.
   * @return this.
   * @throws NullPointerException if either action or attributesMap are null.
   */
  public ActionMatrix addAction(final String action, final AttributeMap attributesMap) throws NullPointerException {
    if (action == null) {
      throw new NullPointerException("action");
    }
    if (attributesMap == null) {
      throw new NullPointerException("attributesMap");
    }

    List<AttributeMap> list = get(action);
    if (list == null) {
      list = new ArrayList<>();
      list.add(attributesMap);
      put(action, list);
      return this;
    }

    if (!list.contains(attributesMap)) {
      list.add(attributesMap);
    }
    return this;
  }

  /**
   * Assumes that this action matrix is used as access rights matrix and compacts the matrix by removing unnecessary attributes maps, which
   * define a subset of the data defined already by other attributes maps.
   * <p>
   * </p>
   * For example if there is one attribute map that does not have any attributes and one that has the attribute "tag" set to "restaurant",
   * then in fact the attribute map with the "tag" set to "restaurant" is needless, because the attributes map without any attributes
   * already grants access rights to all resources what includes those that have the "tag" attribute set to "restaurant".
   */
  public void compactAccessMatrix() {
    final Iterator<Entry<String, List<AttributeMap>>> iterator = entrySet().iterator();
    while (iterator.hasNext()) {
      final Entry<String, List<AttributeMap>> entry = iterator.next();
      final List<AttributeMap> attributeMapList = entry.getValue();
      if (attributeMapList == null || attributeMapList.size() == 0) {
        iterator.remove();
        continue;
      }
      compactFilterList(attributeMapList);
      if (attributeMapList.size() == 0) {
        iterator.remove();
      }
    }
  }

  /**
   * Compacts a list of access attribute maps.
   *
   * @param list the list to be compacted.
   * @throws NullPointerException if the given list is null or empty.
   **/
  private void compactFilterList(final List<AttributeMap> list) throws NullPointerException {
    // Sort the attribute maps by size, for example less specific filters using less properties are in the front, more specific filters
    // using more properties are in the back.
    list.sort(Comparator.comparingInt(HashMap::size));

    // If the "include all" filter "{}" exists, there is no need to check any other elements. All other filters are included in it.
    AttributeMap attributeMap = list.get(0);
    if (attributeMap.size() == 0) {
      if (list.size() > 1) {
        list.clear();
        list.add(attributeMap);
      }
      return;
    }

    for (int i = 0; i < list.size(); i++) {
      attributeMap = list.get(i);

      // The attribute map is a map that will stay in the matrix.
      // We'll use it as a source to see if any of the following (smaller) attribute maps can be removed.
      for (int j = list.size() - 1; j > i; j--) {
        final AttributeMap attributeMapToCheck = list.get(j);

        // Remove filters covered already by more general filters.
        if (attributeMap.matches(attributeMapToCheck, null)) {
          list.remove(j);
        }
      }
    }
  }

  /**
   * Tests this access matrix against the given request matrix.
   *
   * @param requestMatrix the request matrix.
   * @return true if this access matrix grants access to the request; false otherwise.
   */
  public boolean matches(ActionMatrix requestMatrix) {
    if (this.size() == 0) {
      return requestMatrix.size() == 0;
    }
    if (this.size() < requestMatrix.size()) {
      return false;
    }

    // Loop actions.
    for (final Entry<String, List<AttributeMap>> entry : requestMatrix.entrySet()) {
      final String action = entry.getKey();
      if (action == null) {
        return false;
      }
      final List<AttributeMap> resourceList = entry.getValue();
      if (resourceList == null || resourceList.size() == 0) {
        continue;
      }
      final List<AttributeMap> accessList = this.get(action);
      if (accessList == null || accessList.size() == 0) {
        return false;
      }

      // Loop all resources of one action.
      resourceLoop:
      for (final AttributeMap resource : resourceList) {
        if (resource == null) {
          continue;
        }
        for (final AttributeMap access : accessList) {
          if (access == null) {
            continue;
          }
          if (access.size() == 0) {
            continue resourceLoop;
          }
          if (access.matches(resource)) {
            continue resourceLoop;
          }
        }

        // When we reach this point none of the access attribute maps granted access to the resource.
        return false;
      }
    }
    return true;
  }
}
