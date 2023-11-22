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
package com.here.naksha.lib.core.util.diff;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The class provides methods to extract differences from {@link Map} and {@link List} instances,
 * merge differences and patch objects with such differences. All objects not being either a {@link
 * Map} or {@link List} are treated as primitive values.
 */
@SuppressWarnings({"rawtypes", "UnusedReturnValue"})
public class Patcher {

  /**
   * Returns the difference of the two entities or null, if both entities are equal.
   *
   * @param sourceState first object with the source state to be compared against second target
   *                    state.
   * @param targetState the target state against which to compare the source state.
   * @return either null if both objects are equal or the difference that contains what was changed
   * in the target state compared to the source state.
   */
  public static @Nullable Difference getDifference(
      final @Nullable Object sourceState, final @Nullable Object targetState) {
    return getDifference(sourceState, targetState, null);
  }

  /**
   * Returns the difference of the two states or null, if both entities are equal. This method will
   * return {@link InsertOp} if the source state is null, {@link RemoveOp} if the target state is
   * null, {@link MapDiff} if both states are {@link Map maps} that differ, {@link ListDiff} if both
   * states are {@link List lists} that differ and {@link UpdateOp} if the two states are different,
   * but none of them is null and not both of them are {@link Map} or {@link List}.
   *
   * @param sourceState first object with the source state to be compared against second target
   *                    state.
   * @param targetState the target state against which to compare the source state.
   * @param ignoreKey   a method to test for keys to ignore while creating the difference.
   * @return the difference between the two states or null, if both states are equal.
   */
  @SuppressWarnings("WeakerAccess")
  public static @Nullable Difference getDifference(
      final @Nullable Object sourceState,
      final @Nullable Object targetState,
      final @Nullable IgnoreKey ignoreKey) {
    if (sourceState == targetState) {
      return null;
    }

    if (sourceState == null) {
      return new InsertOp(targetState);
    }

    if (targetState == null) {
      return new RemoveOp(sourceState);
    }

    if (sourceState instanceof Map && targetState instanceof Map) {
      return getMapDifference((Map) sourceState, (Map) targetState, ignoreKey);
    }

    if (sourceState instanceof List && targetState instanceof List) {
      return getListDifference((List) sourceState, (List) targetState, ignoreKey);
    }

    if ((sourceState instanceof Number) && (targetState instanceof Number)) {
      Number targetNumber = (Number) targetState;
      Number sourceNumber = (Number) sourceState;
      if ((sourceState instanceof Float)
          || (sourceState instanceof Double)
          || (targetState instanceof Float)
          || (targetState instanceof Double)) {
        if (sourceNumber.doubleValue() == targetNumber.doubleValue()) {
          return null;
        }
      } else if (sourceNumber.longValue() == targetNumber.longValue()) {
        return null;
      }
    }

    // Treat source and target as scalar values.
    if (sourceState.equals(targetState)) {
      return null;
    }

    // The source state was updated to the target state.
    return new UpdateOp(sourceState, targetState);
  }

  /**
   * Returns the difference between two maps.
   *
   * @param sourceState first object to be compared against object B.
   * @param targetState the object against which to compare object A.
   * @param ignoreKey   a method to test for keys to ignore while creating the difference.
   * @return either null if both objects are equal or Difference instance that shows the difference
   * between object A and object B.
   * @throws NullPointerException if the sourceState or targetState is null.
   */
  private static MapDiff getMapDifference(
      final Map sourceState, final Map targetState, final @Nullable IgnoreKey ignoreKey) {
    final MapDiff diff = new MapDiff();

    final Set keysA = sourceState.keySet();
    final Set keysB = targetState.keySet();

    for (final @NotNull Object key : keysA) {
      if (ignoreKey != null && ignoreKey.ignore(key, sourceState, targetState)) {
        continue;
      }

      if (!targetState.containsKey(key)) {
        diff.put(key, new RemoveOp(sourceState.get(key)));
      } else {
        Difference tDiff = getDifference(sourceState.get(key), targetState.get(key), ignoreKey);
        if (tDiff != null) {
          diff.put(key, tDiff);
        }
      }
    }

    for (final @NotNull Object key : keysB) {
      if (sourceState.containsKey(key) || ignoreKey != null && ignoreKey.ignore(key, sourceState, targetState)) {
        continue;
      }

      diff.put(key, new InsertOp(targetState.get(key)));
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
   * @param ignoreKey  a method to test for keys to ignore while creating the difference.
   * @return either null if both lists are equal or Difference instance that shows the difference
   * between list A and list B.
   * @throws NullPointerException if the sourceList or targetList is null.
   */
  private static ListDiff getListDifference(
      final List sourceList, final List targetList, final @Nullable IgnoreKey ignoreKey) {
    int sourceLength = sourceList.size(),
        targetLength = targetList.size(),
        minLen = Math.min(sourceLength, targetLength),
        maxLen = Math.max(sourceLength, targetLength),
        i;
    final ListDiff listDiff = new ListDiff(maxLen);
    listDiff.originalLength = sourceLength;
    listDiff.newLength = targetLength;

    boolean isModified = false;

    // The items that we will find in both lists.
    for (i = 0; i < minLen; i++) {
      final Difference diff = getDifference(sourceList.get(i), targetList.get(i), ignoreKey);
      if (diff != null) {
        isModified = true;
      }
      listDiff.add(diff);
    }

    // If the source (original) list was longer than the target one.
    if (sourceLength > targetLength) {
      isModified = true;
      for (; i < maxLen; i++) {
        listDiff.add(new RemoveOp(sourceList.get(i)));
      }
    }
    // If the target (new) list is longer than the source (original) one.
    else if (targetLength > sourceLength) {
      isModified = true;
      for (; i < maxLen; i++) {
        listDiff.add(new InsertOp(targetList.get(i)));
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
   * @param cr    The conflict resolution strategy.
   * @return a merged difference.
   * @throws MergeConflictException if a conflict that is not automatically solvable occurred.
   */
  public static <T extends Difference> @NotNull Difference mergeDifferences(
      final @NotNull T diffA, final @NotNull T diffB, final @NotNull ConflictResolution cr)
      throws MergeConflictException {
    if (diffA.getClass() != diffB.getClass()) {
      throw new MergeConflictException("Conflict while merging " + diffA + " with " + diffB);
    }

    if (diffA instanceof ListDiff) {
      return mergeListDifferences((ListDiff) diffA, (ListDiff) diffB, cr);
    }

    if (diffA instanceof MapDiff) {
      return mergeMapDifferences((MapDiff) diffA, (MapDiff) diffB, cr);
    }

    if (diffA instanceof UpdateOp || diffA instanceof InsertOp) {
      Object valueA;
      Object valueB;

      if (diffA instanceof UpdateOp) {
        valueA = ((UpdateOp) diffA).newValue();
        valueB = ((UpdateOp) diffB).newValue();
      } else {
        valueA = ((InsertOp) diffA).newValue();
        valueB = ((InsertOp) diffB).newValue();
      }

      if (valueA != valueB) {
        if (valueA == null
            || valueB == null
            || valueA.getClass() != valueB.getClass()
            || !valueA.equals(valueB)) {
          switch (cr) {
            case ERROR:
              throw new MergeConflictException("Conflict while merging " + diffA + " with " + diffB);
            case RETAIN:
              return diffA;
            case REPLACE:
              return diffB;
            default:
              throw new IllegalArgumentException();
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
   * @param cr    The conflict resolution strategy.
   * @return the merged object difference.
   * @throws MergeConflictException if a conflict that is not automatically solvable occurred.
   * @throws MergeConflictException if any error related to JSON occurred.
   */
  private static MapDiff mergeMapDifferences(MapDiff diffA, MapDiff diffB, ConflictResolution cr)
      throws MergeConflictException {
    final MapDiff mergedDiff = new MapDiff();

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
   * @param cr    The conflict resolution strategy.
   * @return the merged list difference.
   * @throws MergeConflictException if a conflict that is not automatically solvable occurred.
   * @throws NullPointerException   if either diffA or diffB are null.
   */
  private static ListDiff mergeListDifferences(ListDiff diffA, ListDiff diffB, ConflictResolution cr)
      throws MergeConflictException {
    final int aLength = diffA.size();
    final int bLength = diffB.size();
    final int MAX = Math.max(aLength, bLength);
    final int MIN = Math.min(aLength, bLength);
    final ListDiff mergeDiff = new ListDiff(MAX);
    mergeDiff.originalLength = diffA.originalLength;

    // Ensure that aDiff is longer than bDiff.
    final ListDiff aDiff;
    final ListDiff bDiff;
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
        final Difference b = i < MIN ? bDiff.get(i) : null;

        // If in any item is found that is remove or insert, we expect that all further items are as
        // well remove or insert
        if ((a instanceof RemoveOp) || (b instanceof RemoveOp)) {
          if (insertFound) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          removeFound = true;
        } else if ((a instanceof InsertOp) || (b instanceof InsertOp)) {
          if (removeFound) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          insertFound = true;
        }

        // If we found remove, we expect only removes
        if (removeFound) {
          if (!(a instanceof RemoveOp)) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          if (i < MIN && !(b instanceof RemoveOp)) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.add(a);
          continue;
        }

        // If we found insert, we expect only inserts
        if (insertFound) {
          if (!(a instanceof InsertOp)) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.newLength++;
          mergeDiff.add(a);

          if (i < MIN) {
            if (!(b instanceof InsertOp)) {
              throw new MergeConflictException(
                  "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
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

        // If in the longer list an item was update, we expect that it was as well updated in the
        // other list
        if (a instanceof UpdateOp) {
          final Object newA = ((UpdateOp) a).newValue();
          if (!(b instanceof UpdateOp)) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }

          final Object newB = ((UpdateOp) b).newValue();
          if (!Objects.equals(newA, newB)) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.add(a);
          continue;
        }

        // If a is an map difference, we expect b to be one either and that we can merge them
        if (a instanceof MapDiff) {
          if (!(b instanceof MapDiff)) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.add(mergeMapDifferences((MapDiff) a, (MapDiff) b, cr));
          continue;
        }

        // If a is an list difference, we expect b to be one either and that we can merge them
        if (a instanceof ListDiff) {
          if (!(b instanceof ListDiff)) {
            throw new MergeConflictException(
                "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
          }
          mergeDiff.add(mergeListDifferences((ListDiff) a, (ListDiff) b, cr));
          continue;
        }

        // We must not reach this point
        throw new MergeConflictException(
            "Conflict while merging " + a + " with " + b + ", collision on index " + i + ".");
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
   * @param object The object to be patched.
   * @param diff   The difference object; if {@code null}, then the unmodified object is returned.
   */
  public static <T> @NotNull T patch(@NotNull T object, final @Nullable Difference diff) {
    if (diff == null) {
      return object;
    }

    if (object instanceof Map) {
      Map map = (Map) object;
      if (!(diff instanceof MapDiff)) {
        throw new IllegalArgumentException(
            "Patch failed, the object is a Map, but the difference is no DiffMap");
      }
      MapDiff mapDiff = (MapDiff) diff;
      //noinspection unchecked
      patchMap(map, mapDiff);
      return object;
    }

    if (object instanceof List) {
      List list = (List) object;
      if (!(diff instanceof ListDiff)) {
        throw new IllegalArgumentException(
            "Patch failed, the object is a List, but the difference is no DiffList");
      }
      //noinspection unchecked
      patchList(list, (ListDiff) diff);
      return object;
    }

    throw new IllegalArgumentException("The given object is no Map or List");
  }

  /**
   * Patches a JSON object with the provided difference object.
   *
   * @param targetMap the map to be patched.
   * @param mapDiff   the difference to be applied to the provided object.
   * @throws NullPointerException     if the provided targetMap is null.
   * @throws IllegalArgumentException if the provided map is no valid Map or MutableMap instance.
   */
  private static void patchMap(final Map<Object, Object> targetMap, final MapDiff mapDiff) {
    if (mapDiff == null) {
      return;
    }

    final Set<Object> keys = mapDiff.keySet();
    for (final Object key : keys) {
      final Object diff = mapDiff.get(key);
      if (diff instanceof InsertOp) {
        targetMap.put(key, ((InsertOp) diff).newValue());
      } else if (diff instanceof RemoveOp) {
        targetMap.remove(key);
      } else if (diff instanceof UpdateOp) {
        targetMap.put(key, ((UpdateOp) diff).newValue());
      } else if (diff instanceof ListDiff) {
        //noinspection unchecked
        patchList((List) targetMap.get(key), (ListDiff) diff);
      } else if (diff instanceof MapDiff) {
        //noinspection unchecked
        patchMap((Map) targetMap.get(key), (MapDiff) diff);
      } else {
        throw new IllegalStateException("The given map contains at key '" + key + "' an invalid element");
      }
    }
  }

  /**
   * Patches a list with the provided difference object.
   *
   * @param list     the array to become merged.
   * @param listDiff the difference to be merged into the provided list.
   * @throws NullPointerException     if the provided list is null.
   * @throws IllegalArgumentException if the provided list is no valid List or MutableList instance.
   */
  private static void patchList(final List<Object> list, final ListDiff listDiff)
      throws NullPointerException, IllegalArgumentException {
    if (listDiff == null) {
      return;
    }
    try {
      for (int i = listDiff.size() - 1; i >= 0; i--) {
        Difference diff = listDiff.get(i);
        if (diff == null) {
          return;
        }

        if (diff instanceof UpdateOp) {
          list.set(i, ((UpdateOp) diff).newValue());
        } else if (diff instanceof RemoveOp) {
          list.remove(i);
        } else if (diff instanceof InsertOp) {
          list.add(((InsertOp) diff).newValue());
        } else if (diff instanceof ListDiff) {
          //noinspection unchecked
          patchList((List<Object>) list.get(i), (ListDiff) diff);
        } else if (diff instanceof MapDiff) {
          //noinspection unchecked
          patchMap((Map<Object, Object>) list.get(i), (MapDiff) diff);
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
   * @param sourceObject  the source object.
   * @param partialUpdate the partial update object.
   * @param ignoreKey     a method to test for keys to ignore while creating the difference.
   * @return the created difference object; {@code null} if no difference found.
   */
  public static @Nullable Difference calculateDifferenceOfPartialUpdate(
      final @NotNull Map sourceObject,
      final @NotNull Map partialUpdate,
      final @Nullable IgnoreKey ignoreKey,
      final boolean recursive) {
    final MapDiff diff = new MapDiff();
    final Set keys = partialUpdate.keySet();
    for (final Object key : keys) {
      if (ignoreKey != null && ignoreKey.ignore(key, sourceObject, partialUpdate)) {
        continue;
      }

      final Object partialUpdateVal = partialUpdate.get(key);
      final Object sourceObjectVal = sourceObject.get(key);
      if (partialUpdateVal == null) {
        if (sourceObject.containsKey(key)) {
          diff.put(key, new RemoveOp(sourceObjectVal));
        }
      } else if (sourceObjectVal == null) {
        diff.put(key, new InsertOp(partialUpdateVal));
      } else if (recursive && sourceObjectVal instanceof Map && partialUpdateVal instanceof Map) {
        final Difference childDiff = calculateDifferenceOfPartialUpdate(
            (Map) sourceObjectVal, (Map) partialUpdateVal, ignoreKey, true);
        if (childDiff != null) {
          diff.put(key, childDiff);
        }
      } else if (!sourceObjectVal.equals(partialUpdateVal)) {
        diff.put(key, new UpdateOp(sourceObjectVal, partialUpdateVal));
      }
    }
    return diff.size() == 0 ? null : diff;
  }
}
