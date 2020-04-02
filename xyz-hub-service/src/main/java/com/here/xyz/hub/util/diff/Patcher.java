/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.util.diff;

import com.here.xyz.hub.task.ModifyOp.IfExists;
import com.here.xyz.hub.util.diff.Difference.DiffList;
import com.here.xyz.hub.util.diff.Difference.DiffMap;
import com.here.xyz.hub.util.diff.Difference.Insert;
import com.here.xyz.hub.util.diff.Difference.Remove;
import com.here.xyz.hub.util.diff.Difference.Update;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * The class provides methods to extract differences from {@link java.util.Map} and {@link java.util.List} instances, merge differences and
 * patch objects with such differences. All objects not being either a {@link java.util.Map} or {@link java.util.List} are treated as
 * primitive values.
 */
public class Patcher {

  /**
   * An empty ignore key list.
   */
  private static final HashMap<Object, Object> EMPTY_IGNORE_KEYS = new HashMap<>();

  /**
   * Returns the difference of the two entities or null, if both entities are equal.
   *
   * @param sourceState first object with the source state to be compared against second target state.
   * @param targetState the target state against which to compare the source state.
   * @return either null if both objects are equal or the difference that contains what was changed in the target state compared to the
   * source state.
   */
  public static Difference getDifference(final Object sourceState, final Object targetState) {
    return getDifference(sourceState, targetState, EMPTY_IGNORE_KEYS);
  }

  /**
   * Returns the difference of the two states or null, if both entities are equal. This method will return {@link Insert} if the source
   * state is null, {@link Remove} if the target state is null, {@link DiffMap} if both states are {@link java.util.Map maps} that differ,
   * {@link DiffList} if both states are {@link java.util.List lists} that differ and {@link Update} if the two states are different, but
   * none of them is null and not both of them are {@link java.util.Map} or {@link java.util.List}.
   *
   * @param sourceState first object with the source state to be compared against second target state.
   * @param targetState the target state against which to compare the source state.
   * @param ignoreKeys an optional map of all keys to be ignored while calculating the difference of two maps.
   * @return the difference between the two states or null, if both states are equal.
   */
  @SuppressWarnings("WeakerAccess")
  public static Difference getDifference(final Object sourceState, final Object targetState, final Map<Object, Object> ignoreKeys) {
    // If both objects are the same instance or null, they are the same
    if (sourceState == targetState) {
      return null;
    }

    // If the source state is null, then the target state was inserted
    if (sourceState == null) {
      return new Insert(targetState);
    }

    // If the target state is null, then the source state was removed
    if (targetState == null) {
      return new Remove(sourceState);
    }

    // if both objects are maps, return an object difference
    if (sourceState instanceof Map && targetState instanceof Map) {
      return getMapDifference((Map) sourceState, (Map) targetState, ignoreKeys);
    }

    // if both objects are lists, return an array difference
    if (sourceState instanceof List && targetState instanceof List) {
      return getListDifference((List) sourceState, (List) targetState, ignoreKeys);
    }

    // if both objects are equal, there is no difference
    if (sourceState.equals(targetState)) {
      return null;
    }

    // if source and target are numbers
    if ((sourceState instanceof Number) && (targetState instanceof Number)) {
      if ((sourceState instanceof Float) || (sourceState instanceof Double) || (targetState instanceof Float)
          || (targetState instanceof Double)) {
        if (((Number) sourceState).doubleValue() == ((Number) targetState).doubleValue()) {
          return null;
        }
      }
      // We are sure we do not have a floating point number, so compare the long values of the numbers.
      else if ((((Number) sourceState).longValue() == ((Number) targetState).longValue())) {
        return null;
      }
    }

    // otherwise the source state was updated to the target state
    return new Update(sourceState, targetState);
  }

  /**
   * Returns the difference between two maps.
   *
   * @param sourceState first object to be compared against object B.
   * @param targetState the object against which to compare object A.
   * @param ignoreKeys an optional list of keys to be ignored while constructing the difference object.
   * @return either null if both objects are equal or Difference instance that shows the difference between object A and object B.
   * @throws NullPointerException if the sourceState or targetState is null.
   */
  private static DiffMap getMapDifference(final Map sourceState, final Map targetState, Map<Object, Object> ignoreKeys)
      throws NullPointerException {
    final DiffMap diff = new DiffMap();
    if (ignoreKeys == null) {
      ignoreKeys = EMPTY_IGNORE_KEYS;
    }

    final Set keysA = sourceState.keySet();
    final Set keysB = targetState.keySet();

    for (Object key : keysA) {
      if (ignoreKeys.containsKey(key)) {
        continue;
      }

      if (!targetState.containsKey(key)) {
        diff.put(key, new Remove(sourceState.get(key)));
      } else {
        Difference tDiff = getDifference(sourceState.get(key), targetState.get(key), ignoreKeys);
        if (tDiff != null) {
          diff.put(key, tDiff);
        }
      }
    }

    for (Object key : keysB) {
      if (ignoreKeys.containsKey(key) || sourceState.containsKey(key)) {
        continue;
      }

      diff.put(key, new Insert(targetState.get(key)));
    }

    if (diff.size() == 0) {
      return null;
    }

    return diff;
  }

  /**
   * Returns the difference of the two list objects.
   *
   * @param sourceList first list to be compared against list B.
   * @param targetList the list against which to compare list A.
   * @param ignoreKeys an optional map, whose keys are going to be ignored while constructing the difference object.
   * @return either null if both lists are equal or Difference instance that shows the difference between list A and list B.
   * @throws NullPointerException if the sourceList or targetList is null.
   */
  private static DiffList getListDifference(final List sourceList, final List targetList, final Map<Object, Object> ignoreKeys)
      throws NullPointerException {
    int sourceLength = sourceList.size(),
        targetLength = targetList.size(),
        minLen = Math.min(sourceLength, targetLength),
        maxLen = Math.max(sourceLength, targetLength), i;
    final DiffList listDiff = new DiffList(maxLen);
    listDiff.originalLength = sourceLength;
    listDiff.newLength = targetLength;

    boolean isModified = false;

    // The items that we will find in both lists.
    for (i = 0; i < minLen; i++) {
      final Difference diff = getDifference(sourceList.get(i), targetList.get(i), ignoreKeys);
      if (diff != null) {
        isModified = true;
      }
      listDiff.add(diff);
    }

    // If the source (original) list was longer than the target one.
    if (sourceLength > targetLength) {
      isModified = true;
      for (; i < maxLen; i++) {
        listDiff.add(new Remove(sourceList.get(i)));
      }
    }
    // If the target (new) list is longer than the source (original) one.
    else if (targetLength > sourceLength) {
      isModified = true;
      for (; i < maxLen; i++) {
        listDiff.add(new Insert(targetList.get(i)));
      }
    }

    // If nothing changed, so we have an array full of null, there is no change.
    if (!isModified) {
      return null;
    }

    return listDiff;
  }

  /**
   * Merge two differences, both differences must be of the same type.
   *
   * @param diffA the first difference.
   * @param diffB the second difference.
   * @return a merged difference.
   * @throws MergeConflictException if a conflict that is not automatically solvable occurred.
   */
  public static <T extends Difference> Difference mergeDifferences(final T diffA, final T diffB, ConflictResolution cr ) throws MergeConflictException {
    if (diffA == null) {
      return diffB;
    }

    if (diffB == null) {
      return diffA;
    }

    if (diffA.getClass() != diffB.getClass()) {
      throw new MergeConflictException("Conflict while merging " + diffA + " with " + diffB);
    }

    if (diffA instanceof DiffList) {
      return mergeListDifferences((DiffList) diffA, (DiffList) diffB, cr);
    }

    if (diffA instanceof DiffMap) {
      return mergeMapDifferences((DiffMap) diffA, (DiffMap) diffB, cr);
    }

    if (diffA instanceof Update) {
      final Object valueA = ((Update) diffA).newValue();
      final Object valueB = ((Update) diffB).newValue();
      if (valueA != valueB) {
        if (valueA == null || valueB == null || valueA.getClass() != valueB.getClass() || !valueA.equals(valueB)) {
          switch(cr) {
            case ERROR:
              throw new MergeConflictException("Conflict while merging " + diffA + " with " + diffB);
            case RETAIN:
              return diffA;
            case REPLACE:
              return diffB;
          }
        }
      }
    }

    return diffA;
  }

  /**
   * Internal method to merge two objects differences.
   *
   * @param diffA an object difference.
   * @param diffB another object difference.
   * @param cr
   * @return the merged object difference.
   * @throws MergeConflictException if a conflict that is not automatically solvable occurred.
   * @throws MergeConflictException if any error related to JSON occurred.
   */
  private static DiffMap mergeMapDifferences(DiffMap diffA, DiffMap diffB, ConflictResolution cr) throws MergeConflictException {
    final DiffMap mergedDiff = new DiffMap();

    Set<Object> diffAKeys = diffA.keySet();
    for (Object key : diffAKeys) {
      mergedDiff.put(key, mergeDifferences(diffA.get(key), diffB.get(key), cr));
    }

    Set<Object> diffBKeys = diffB.keySet();
    for (Object key : diffBKeys) {
      if (!mergedDiff.containsKey(key)) {
        mergedDiff.put(key, diffB.get(key));
      }
    }

    return mergedDiff;
  }

  /**
   * Internal method to merge two list differences.
   *
   * @param diffA a list difference.
   * @param diffB another list difference.
   * @param cr
   * @return the merged list difference.
   * @throws MergeConflictException if a conflict that is not automatically solvable occurred.
   * @throws NullPointerException if either diffA or diffB are null.
   */
  private static DiffList mergeListDifferences(DiffList diffA, DiffList diffB, ConflictResolution cr) throws MergeConflictException {
    final int aLength = diffA.size();
    final int bLength = diffB.size();
    final int MAX = Math.max(aLength, bLength);
    final int MIN = Math.min(aLength, bLength);
    final DiffList mergeDiff = new DiffList(MAX);
    mergeDiff.originalLength = diffA.originalLength;

    // Ensure that aDiff is longer than bDiff.
    final DiffList aDiff;
    final DiffList bDiff;
    if (bLength > aLength) {
      aDiff = diffB;
      bDiff = diffA;
    } else {
      aDiff = diffA;
      bDiff = diffB;
    }

    try {
      boolean removeFound = false;
      boolean insertFound = false;
      for (int i = 0; i < aDiff.size(); i++) {
        Difference a = aDiff.get(i);
        // As b might be shorter than a, we treat any missing element in b as an "unchanged"
        final Difference b = i < MIN
            ? bDiff.get(i) : null;

        // If in any item is found that is remove or insert, we expect that all further items are as well remove or insert
        if ((a instanceof Remove) || (b instanceof Remove)) {
          if (insertFound) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          removeFound = true;
        } else if ((a instanceof Insert) || (b instanceof Insert)) {
          if (removeFound) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          insertFound = true;
        }

        // If we found remove, we expect only removes
        if (removeFound) {
          if (!(a instanceof Remove)) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          if (i < MIN && !(b instanceof Remove)) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.add(a);
          continue;
        }

        // If we found insert, we expect only inserts
        if (insertFound) {
          if (!(a instanceof Insert)) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.newLength++;
          mergeDiff.add(a);

          if (i < MIN) {
            if (!(b instanceof Insert)) {
              throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
            }
            mergeDiff.newLength++;
            mergeDiff.add(b);
          }
          continue;
        }

        // If the value was not changed in a
        if (a == null) {
          // Any update done in b is acceptable
          mergeDiff.newLength++;
          mergeDiff.add(b);
          continue;
        }

        // If the value was not changed in b
        if (b == null) {
          // Any update done in a is acceptable
          mergeDiff.newLength++;
          mergeDiff.add(a);
          continue;
        }

        // If in the longer list an item was update, we expect that it was as well updated in the other list
        if (a instanceof Update) {
          final Object newA = ((Update) a).newValue();
          if (!(b instanceof Update)) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }

          final Object newB = ((Update) b).newValue();
          if (!Objects.equals(newA, newB)) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.add(a);
          continue;
        }

        // If a is an map difference, we expect b to be one either and that we can merge them
        if (a instanceof DiffMap) {
          if (!(b instanceof DiffMap)) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.add(mergeMapDifferences((DiffMap) a, (DiffMap) b, cr));
          continue;
        }

        // If a is an list difference, we expect b to be one either and that we can merge them
        if (a instanceof DiffList) {
          if (!(b instanceof DiffList)) {
            throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.add(mergeListDifferences((DiffList) a, (DiffList) b, cr));
          continue;
        }

        // We must not reach this point
        throw new MergeConflictException("Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
      }
    } catch (MergeConflictException e) {
      throw e;
    } catch (Exception e) {
      throw new MergeConflictException("Unexpected exception while merging:\n" + ExceptionUtils.getStackTrace(e));
    }
    return mergeDiff;
  }

  /**
   * Patches the map with the provided difference object.
   *
   * @param obj The object to be patched.
   * @param diff The difference object.
   */
  public static void patch(Object obj, final Difference diff) {
    if( diff == null )
      return;

    if (obj instanceof Map) {
      if (!(diff instanceof DiffMap)) {
        throw new IllegalArgumentException("Patch failed, the object is a Map, but the difference is no DiffMap");
      }
      //noinspection unchecked
      patchMap((Map<Object, Object>) obj, (DiffMap) diff);
    } else if (obj instanceof List) {
      if (!(diff instanceof DiffList)) {
        throw new IllegalArgumentException("Patch failed, the object is a List, but the difference is no DiffList");
      }
      //noinspection unchecked
      patchList((List) obj, (DiffList) diff);
    } else {
      throw new IllegalArgumentException("The given object is no Map or List");
    }
  }

  /**
   * Patches a JSON object with the provided difference object.
   *
   * @param targetMap the map to be patched.
   * @param mapDiff the difference to be applied to the provided object.
   * @throws NullPointerException if the provided targetMap is null.
   * @throws IllegalArgumentException if the provided map is no valid Map or MutableMap instance.
   */
  private static void patchMap(final Map<Object, Object> targetMap, final DiffMap mapDiff)
      throws NullPointerException, IllegalArgumentException {
    if (mapDiff == null) {
      return;
    }

    final Set<Object> keys = mapDiff.keySet();
    for (final Object key : keys) {
      final Object diff = mapDiff.get(key);
      if (diff instanceof Insert) {
        targetMap.put(key, ((Insert) diff).newValue());
      } else if (diff instanceof Remove) {
        targetMap.remove(key);
      } else if (diff instanceof Update) {
        targetMap.put(key, ((Update) diff).newValue());
      } else if (diff instanceof DiffList) {
        //noinspection unchecked
        patchList((List) targetMap.get(key), (DiffList) diff);
      } else if (diff instanceof DiffMap) {
        //noinspection unchecked
        patchMap((Map) targetMap.get(key), (DiffMap) diff);
      } else {
        throw new IllegalStateException("The given map contains at key '" + key + "' an invalid element");
      }
    }
  }

  /**
   * Patches a list with the provided difference object.
   *
   * @param list the array to become merged.
   * @param listDiff the difference to be merged into the provided list.
   * @throws NullPointerException if the provided list is null.
   * @throws IllegalArgumentException if the provided list is no valid List or MutableList instance.
   */
  private static void patchList(final List<Object> list, final DiffList listDiff) throws NullPointerException, IllegalArgumentException {
    if (listDiff == null) {
      return;
    }
    try {
      for (int i = listDiff.size() - 1; i >= 0; i--) {
        Difference diff = listDiff.get(i);
        if (diff == null) {
          return;
        }

        if (diff instanceof Update) {
          list.set(i, ((Update) diff).newValue());
        } else if (diff instanceof Remove) {
          list.remove(i);
        } else if (diff instanceof Insert) {
          list.add(((Insert) diff).newValue());
        } else if (diff instanceof DiffList) {
          //noinspection unchecked
          patchList((List<Object>) list.get(i), (DiffList) diff);
        } else if (diff instanceof DiffMap) {
          //noinspection unchecked
          patchMap((Map<Object, Object>) list.get(i), (DiffMap) diff);
        } else {
          throw new IllegalStateException("The given list contains at index #" + i + " an invalid element");
        }
      }
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Unexpected exception in visitor", e);
    }
  }

  /**
   * Converts a partial update object into a difference object.
   *
   * @param <K> the type of the map keys.
   * @param <V> the type of the map values.
   * @param sourceObject the source object.
   * @param partialUpdate the partial update object.
   * @param ignoreKeys the keys to be ignored from the partial update object.
   * @return the created difference object.
   */
  @SuppressWarnings({"unchecked"})
  public static <K, V> Difference calculateDifferenceOfPartialUpdate(final Map<K, V> sourceObject, final Map<K, V> partialUpdate,
      final Map<K, K> ignoreKeys, final boolean recursive) {

    DiffMap diff = new DiffMap();

    if (partialUpdate == null) {
      return diff;
    }

    Set<K> keys = partialUpdate.keySet();
    for (K key : keys) {
      if (ignoreKeys != null && ignoreKeys.containsKey(key)) {
        continue;
      }

      final V partialUpdateVal = partialUpdate.get(key);
      final V sourceObjectVal = sourceObject.get(key);
      if (partialUpdateVal == null) {
        if (sourceObject.containsKey(key)) {
          diff.put(key, new Remove(sourceObjectVal));
        }
      } else if (sourceObjectVal == null ) {
        diff.put(key, new Insert(partialUpdateVal));
      } else if (recursive && sourceObjectVal instanceof Map && partialUpdateVal instanceof Map) {
        Difference childDiff = calculateDifferenceOfPartialUpdate((Map<K, V>) sourceObjectVal, (Map<K, V>) partialUpdateVal, ignoreKeys,true);
        if( childDiff != null )
          diff.put(key, childDiff);
      } else if (!sourceObjectVal.equals(partialUpdateVal)) {
        diff.put(key, new Update(sourceObjectVal, partialUpdateVal));
      }
    }
    return diff.size() == 0 ? null : diff;
  }

  /**
   * An exception thrown if applying a patch fails, the creation of a difference fails or any other merge error occurrs.
   */
  public static class MergeConflictException extends Exception {

    MergeConflictException(String msg) {
      super(msg);
    }
  }

  public enum ConflictResolution{
    ERROR,
    RETAIN,
    REPLACE;

    public static ConflictResolution of(String value) {
      if (value == null) {
        return null;
      }
      try {
        return valueOf(value.toUpperCase());
      } catch (IllegalArgumentException e) {
        return null;
      }
    }
  }
}
