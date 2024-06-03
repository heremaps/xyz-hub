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
package com.here.naksha.lib.core.models.auth;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A map of attributes used to describe the attributes of a resource.
 *
 * If used in a request matrix, all defined attributes for the corresponding action must be set as defined in the service declaration. If
 * the value of an attribute is an array, this means that the application/user need to have rights for <b>all</b> these values.
 *
 * If used in an access matrix, then not defined attributes mean "catch all", so the application/user does have rights for all values. If
 * the value of an attribute is an array, this means that the application/user does have rights for <b>all</b> of the values.
 */
public class AttributeMap extends LinkedHashMap<String, Object> {

  /**
   * Constant for the "wildcard" character.
   */
  public static final String WILDCARD = "*";

  /**
   * Creates a new empty attributes map.
   */
  public AttributeMap() {}

  /**
   * A helper method to test if the given value is a scalar value that is allowed in a attributes map.
   *
   * @param value the value to test.
   * @return true if the value is a scalar value that is allowed; false otherwise.
   */
  private static boolean isScalar(Object value) {
    return (value == null) || (value instanceof Number) || (value instanceof String) || (value instanceof Boolean);
  }

  /**
   * A helper method to test if the given value is a valid value for the attribute map, therefore either being a scalar value, or a {@link
   * List} only containing valid scalar values.
   *
   * @param value the value to test.
   * @return true if the value is allowed in the attributes map; false otherwise.
   */
  @SuppressWarnings("unchecked")
  public static boolean isValidValue(Object value) {
    if (value instanceof List) {
      final List<Object> list = (List<Object>) value;
      for (final Object v : list) {
        if (!isScalar(v)) {
          return false;
        }
      }
      return true;
    }

    return isScalar(value);
  }

  /**
   * A helper method that compares a scalar value from the access attributes with a scalar value of the resource attributes.
   *
   * @param accessValue the scalar access attribute value.
   * @param resourceValue the scalar resource attribute value.
   * @return true if the access value matches the resource values; false otherwise.
   */
  private static boolean matchScalarVsScalar(Object accessValue, Object resourceValue) {
    if (accessValue == resourceValue) {
      return true;
    }
    if (accessValue == null) {
      return false;
    }

    if (accessValue.equals(resourceValue)) {
      return true;
    }

    // If there was no direct match, but the access value and resource value are strings, we need to check if the
    // access value ends with
    // the wildcard character and if it does, we need to compare only the sub-string up to the "wildcard" character.
    if ((accessValue instanceof String) && (resourceValue instanceof String)) {
      final String accessValueString = (String) accessValue;
      if (accessValueString.endsWith(WILDCARD)) {
        final String resourceValueString = (String) resourceValue;
        return substringEquals(accessValueString, 0, resourceValueString, 0, accessValueString.length() - 1);
      }
    }
    return false;
  }

  /**
   * A helper method that compares a single value from the access attributes with a list of values from the resource attributes.
   *
   * @param accessValue the scalar access attribute value.
   * @param resourceValues the list of resource values.
   * @return true if the access value matches at least one of the resource values; false otherwise.
   */
  private static boolean matchScalarInList(Object accessValue, List<Object> resourceValues) {
    if (resourceValues == null || resourceValues.size() == 0) {
      return false;
    }
    if (resourceValues.contains(accessValue)) {
      return true;
    }

    // If there was no direct match, but the access value is a string, check for wildcards.
    if (accessValue instanceof String) {
      final String accessValueString = (String) accessValue;
      if (accessValueString.endsWith(WILDCARD)) {
        for (int i = 0; i < resourceValues.size(); i++) {
          final Object resourceValue = resourceValues.get(i);
          if (resourceValue instanceof String) {
            final String resourceValueString = (String) resourceValue;
            if (substringEquals(
                accessValueString, 0, resourceValueString, 0, accessValueString.length() - 1)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  /**
   * A helper method that compares all values of access attributes with the values of resource attributes.
   *
   * @param accessValues the access attribute values; if null, then false is returned.
   * @param resourceValue the resource attribute value being either a single value (including null) or a list of scalar values.
   * @return true if each access values matches at least one of the resource values; false otherwise.
   */
  @SuppressWarnings("unchecked")
  private static boolean matchAll(List<Object> accessValues, final Object resourceValue) {
    if (accessValues == null || accessValues.size() == 0) {
      return false;
    }

    if (resourceValue instanceof List) {
      final List<Object> resourceValues = (List<Object>) resourceValue;
      for (final Object accessValue : accessValues) {
        if (!matchScalarInList(accessValue, resourceValues)) {
          return false;
        }
      }
    } else {
      for (final Object accessValue : accessValues) {
        if (!matchScalarVsScalar(accessValue, resourceValue)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Tests whether the access attributes match the given resource attributes. The method will compare all key-value pairs of the access
   * attributes against the corresponding key-value pairs of the resource attributes map. If the resource attributes contain more key-value
   * pairs than the access attributes map this is ignored, but if the access attributes map contains more key-value pairs than the resource
   * attributes map, this means that the access is limited something the resource does not fulfill, therefore the method will return false.
   * </p>
   * <p>
   * For example a user has the right to edit all features that have the attribute "tag" set to "restaurant". In this case access should be
   * granted even when the resource attributes map of the accessed feature does have more attributes like e.g. "open24h". However, if the
   * resource is missing the "tag" attribute or the "restaurant" tag it clearly seems not to have the desired properties, therefore the
   * access must be rejected. This means (implicitly) that an empty access attribute map grants access to all resources.
   * </p>
   * <p>
   * If the value of an attribute is an array, then it is treated as an AND clause and means for the access attributes map that the resource
   * must have all values in the corresponding key. If the value of the resource attribute is an array it means that the corresponding
   * attribute of the resource does have multiple values, for example a resource may have three values for the attribute "tag" being
   * "restaurant", "burger" and "pizza".
   *
   * @param accessAttributesMap the attributes map that describes the access rights for the resource; if null, the method will return
   * false.
   * @param resourceAttributesMap the map that describes the attributes of the resource.
   * @param ignoreKeys an optional map of keys to ignore when reading from the access attributes map; may be null.
   * @return true if the access attributes map matches the given resource attributes map; false otherwise.
   */
  @SuppressWarnings("unchecked")
  private static boolean matches(
      final Map<String, Object> accessAttributesMap,
      final Map<String, Object> resourceAttributesMap,
      final Set<String> ignoreKeys) {
    if (accessAttributesMap == null) {
      return false;
    }
    for (final String key : accessAttributesMap.keySet()) {
      if (ignoreKeys != null && ignoreKeys.contains(key)) {
        continue;
      }
      if (resourceAttributesMap == null) {
        return false;
      }

      Object accessValue = accessAttributesMap.get(key);
      Object objectValue = resourceAttributesMap.get(key);

      if (accessValue instanceof List) {
        final List<Object> accessValues = (List<Object>) accessValue;
        if (!matchAll(accessValues, objectValue)) {
          return false;
        }
      } else if (objectValue instanceof List) {
        if (!matchScalarInList(accessValue, (List<Object>) objectValue)) {
          return false;
        }
      } else {
        if (!matchScalarVsScalar(accessValue, objectValue)) {
          return false;
        }
      }
    }
    return true;
  }

  public static boolean substringEquals(String str1, int start1, String str2, int start2, int length) {
    if (str1 == null || str2 == null || str1.length() < start1 + length || str2.length() < start2 + length) {
      return false;
    }

    for (int i = 0; i < length; i++) {
      if (str1.charAt(i + start1) != str2.charAt(i + start2)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public Object put(String key, Object value) {
    if (key == null) {
      throw new NullPointerException("key");
    }
    if (!isValidValue(value)) {
      throw new IllegalArgumentException("value");
    }
    return super.put(key, value);
  }

  @Override
  public void putAll(Map<? extends String, ?> m) {
    if (m != null) {
      for (final Map.Entry<? extends String, ?> entry : m.entrySet()) {
        final String key = entry.getKey();
        if (key == null) {
          throw new NullPointerException("key");
        }
        final Object value = entry.getValue();
        if (!isValidValue(value)) {
          throw new IllegalArgumentException("Illegal value for key '" + key + "'");
        }
      }
    }
    super.putAll(m);
  }

  @Override
  public Object putIfAbsent(String key, Object value) {
    if (key == null) {
      throw new NullPointerException("key");
    }
    if (!isValidValue(value)) {
      throw new IllegalArgumentException("value");
    }
    return super.putIfAbsent(key, value);
  }

  @Override
  public boolean replace(String key, Object oldValue, Object newValue) {
    if (key == null) {
      throw new NullPointerException("key");
    }
    if (!isValidValue(newValue)) {
      throw new IllegalArgumentException("newValue");
    }
    return super.replace(key, oldValue, newValue);
  }

  @Override
  public Object replace(String key, Object value) {
    if (!isValidValue(value)) {
      throw new IllegalArgumentException("value");
    }
    return super.replace(key, value);
  }

  /**
   * Sets the given key to the given value and returns this instance again.
   *
   * @param key the key to be set.
   * @param value the value to be set.
   * @return this.
   * @throws NullPointerException if the given key is null.
   * @throws IllegalArgumentException if the given value is not allowed.
   */
  public AttributeMap withValue(final String key, final Object value) {
    if (key == null) {
      throw new NullPointerException("key");
    }
    if (!isValidValue(value)) {
      throw new IllegalArgumentException("value");
    }
    put(key, value);
    return this;
  }

  /**
   * Adds the given key and the the given value and returns this instance again. If there is already a value for the given key, the given
   * value will be added and the current value is optionally converted into a {@link List} of values. The method will not add the same value
   * twice.
   *
   * @param key the key to be added.
   * @param value the value to be added, must be an scalar value.
   * @return this.
   * @throws NullPointerException if the given key is null.
   * @throws IllegalArgumentException if the given value is not allowed.
   */
  @SuppressWarnings("unchecked")
  public AttributeMap addValue(final String key, final Object value) {
    if (key == null) {
      throw new NullPointerException("key");
    }

    if (!containsKey(key)) {
      return withValue(key, value);
    }

    if (!isScalar(value)) {
      throw new IllegalArgumentException("value");
    }

    List<Object> list;
    final Object raw = get(key);
    if (raw instanceof List) {
      list = (List<Object>) raw;
    } else {
      if (Objects.equals(raw, value)) {
        return this;
      }
      list = new ArrayList<>();
      list.add(raw);
      list.add(value);
      super.put(key, list);
      return this;
    }

    if (!list.contains(value)) {
      list.add(value);
    }
    return this;
  }

  /**
   * Tests whether this access attributes match the given resource attributes. The method will compare all key-value pairs of the access
   * attributes against the corresponding key-value pairs of the resource attributes map. If the resource attributes contain more key-value
   * pairs than the access attributes map this is ignored, but if the access attributes map contains more key-value pairs than the resource
   * attributes map, this means that the access is limited something the resource does not fulfill, therefore the method will return false.
   * </p>
   * <p>
   * For example a user has the right to edit all features that have the attribute "tag" set to "restaurant". In this case access should be
   * granted even when the resource attributes map of the accessed feature does have more attributes like e.g. "open24h". However, if the
   * resource is missing the "tag" attribute or the "restaurant" tag it clearly seems not to have the desired properties, therefore the
   * access must be rejected. This means (implicitly) that an empty access attribute map grants access to all resources.
   * </p>
   * <p>
   * If the value of an attribute is an array, then it is treated as an AND clause and means for the access attributes map that the resource
   * must have all values in the corresponding key. If the value of the resource attribute is an array it means that the corresponding
   * attribute of the resource does have multiple values, for example a resource may have three values for the attribute "tag" being
   * "restaurant", "burger" and "pizza".
   *
   * @param resourceAttributesMap the map that describes the attributes of the resource.
   * @return true if the access attributes map matches the given resource attributes map; false otherwise.
   */
  public boolean matches(final Map<String, Object> resourceAttributesMap) {
    return matches(resourceAttributesMap, null);
  }

  /**
   * Tests whether this access attributes match the given resource attributes. The method will compare all key-value pairs of the access
   * attributes against the corresponding key-value pairs of the resource attributes map. If the resource attributes contain more key-value
   * pairs than the access attributes map this is ignored, but if the access attributes map contains more key-value pairs than the resource
   * attributes map, this means that the access is limited something the resource does not fulfill, therefore the method will return false.
   * </p>
   * <p>
   * For example a user has the right to edit all features that have the attribute "tag" set to "restaurant". In this case access should be
   * granted even when the resource attributes map of the accessed feature does have more attributes like e.g. "open24h". However, if the
   * resource is missing the "tag" attribute or the "restaurant" tag it clearly seems not to have the desired properties, therefore the
   * access must be rejected. This means (implicitly) that an empty access attribute map grants access to all resources.
   * </p>
   * <p>
   * If the value of an attribute is an array, then it is treated as an AND clause and means for the access attributes map that the resource
   * must have all values in the corresponding key. If the value of the resource attribute is an array it means that the corresponding
   * attribute of the resource does have multiple values, for example a resource may have three values for the attribute "tag" being
   * "restaurant", "burger" and "pizza".
   *
   * @param resourceAttributesMap the map that describes the attributes of the resource.
   * @param ignoreKeys an optional map of keys to ignore when reading from the access attributes map; may be null.
   * @return true if the access attributes map matches the given resource attributes map; false otherwise.
   */
  public boolean matches(final Map<String, Object> resourceAttributesMap, final Set<String> ignoreKeys) {
    return AttributeMap.matches(this, resourceAttributesMap, ignoreKeys);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Map)) {
      return false;
    }
    final Map<String, Object> o = (Map<String, Object>) other;
    if (size() != o.size()) {
      return false;
    }
    try {
      for (final String key : keySet()) {
        if (!o.containsKey(key)) {
          return false;
        }
        final Object value = get(key);
        final Object v = o.get(key);
        if (value == v) {
          continue;
        }
        if (value == null || v == null) {
          return false;
        }
        if (value.equals(v)) {
          continue;
        }
        if ((value instanceof List) && (v instanceof List)) {
          final List<Object> thisList = (List<Object>) value;
          final List<Object> oList = (List<Object>) v;
          if (thisList.size() != oList.size()) {
            return false;
          }
          for (int i = 0; i < thisList.size(); i++) {
            final Object thisValue = thisList.get(i);
            final Object oValue = oList.get(i);
            if (thisValue == oValue) {
              continue;
            }
            if (thisValue == null || !isScalar(thisValue) || !thisValue.equals(oValue)) {
              return false;
            }
          }
        }
        return false;
      }
      return true;
    } catch (ClassCastException e) {
      return false;
    }
  }
}
