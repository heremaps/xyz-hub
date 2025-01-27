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
package com.here.naksha.storage.http.connector.pop;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.models.payload.events.PropertyQuery.QueryOperation.*;
import static com.here.naksha.lib.core.models.payload.events.PropertyQuery.QueryOperation.CONTAINS;
import static com.here.naksha.lib.core.models.storage.POpType.*;

import com.here.naksha.lib.core.models.payload.events.PropertyQuery;
import com.here.naksha.lib.core.models.payload.events.PropertyQueryAnd;
import com.here.naksha.lib.core.models.payload.events.PropertyQueryOr;
import com.here.naksha.lib.core.models.storage.OpType;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import java.util.*;
import org.jetbrains.annotations.NotNull;

class POpToPropertiesQuery {

  private static final String NULL = null;
  private static final String PATH_SEGMENT_DELIMITER = ".";

  private static final Map<OpType, PropertyQuery.QueryOperation> SIMPLE_LEAF_OPERATORS = Map.of(
      POpType.EQ, EQUALS,
      POpType.GT, GREATER_THAN,
      POpType.GTE, GREATER_THAN_OR_EQUALS,
      POpType.LT, LESS_THAN,
      POpType.LTE, LESS_THAN_OR_EQUALS,
      POpType.CONTAINS, CONTAINS // Connector cannot handle this operation type, but DataHub works the same
      );

  private POpToPropertiesQuery() {}

  static PropertyQueryOr toPopQueryOr(POp pOp) {
    PropertyQueryOr queryOr = new PropertyQueryOr();
    PropertyQueryAnd propertyQueryAnd = toPoPQueryAnd(pOp);
    queryOr.add(propertyQueryAnd);
    return queryOr;
  }

  static PropertyQueryAnd toPoPQueryAnd(POp pOp) {
    if (pOp.op() == AND) {
      return and(pOp);
    } else {
      PropertyQueryAnd propertyQueries = new PropertyQueryAnd();
      propertyQueries.add(pOpToMultiValueComparison(pOp));
      return propertyQueries;
    }
  }

  private static PropertyQueryAnd and(POp pOp) {
    assertHasAtLeastOneChildren(pOp);
    List<PropertyQuery> list = pOp.children().stream()
        .flatMap((child) -> toPoPQueryAnd(child).stream())
        .toList();
    PropertyQueryAnd propertyQueries = new PropertyQueryAnd();
    propertyQueries.addAll(list);
    return propertyQueries;
  }

  private static PropertyQuery pOpToMultiValueComparison(POp pOp) {
    if (pOp.op() == AND) throw unsupportedOperation("AND can be only a top level operation");
    if (pOp.op() == OR) return or(pOp);
    if (pOp.op() == NOT) return not(pOp);
    if (pOp.op() == EXISTS) return exists(pOp);
    return simpleLeafOperator(pOp);
  }

  private static PropertyQuery or(POp pOp) {
    assertHasAtLeastOneChildren(pOp);

    return pOp.children().stream()
        .map(POpToPropertiesQuery::pOpToMultiValueComparison)
        .reduce((PropertyQuery l, PropertyQuery r) -> {
          if (!Objects.equals(l.getOperation(), r.getOperation()))
            throw unsupportedOperation(
                "Operators " + l.getOperation() + " and " + r.getOperation() + " combined in one OR");
          if (!Objects.equals(l.getKey(), r.getKey()))
            throw unsupportedOperation(
                "Operator OR with dwo different keys: " + l.getKey() + " and " + r.getKey());
          List<Object> list =
              new ArrayList<>(l.getValues().size() + r.getValues().size());
          list.addAll(l.getValues());
          list.addAll(r.getValues());
          return new PropertyQuery(l.getKey(), l.getOperation()).withValues(list);
        })
        .orElseThrow(() -> new IllegalStateException("Should not reach here."));
  }

  private static PropertyQuery not(POp pOp) {
    assertHasNChildren(pOp, 1);
    PropertyQuery multiValueComparison =
        pOpToMultiValueComparison(pOp.children().get(0));
    PropertyQuery.QueryOperation newOperator;
    if (multiValueComparison.getOperation().equals(EQUALS)) {
      newOperator = NOT_EQUALS;
    } else if (multiValueComparison.getOperation().equals(NOT_EQUALS)) {
      newOperator = EQUALS;
    } else {
      throw unsupportedOperation("Cannot negate operation: " + multiValueComparison.getOperation());
    }
    return new PropertyQuery(multiValueComparison.getKey(), newOperator)
        .withValues(multiValueComparison.getValues());
  }

  private static PropertyQuery exists(POp pOp) {
    assertHasNChildren(pOp, 0);
    assertHasPathSet(pOp);

    return new PropertyQuery(joinPath(pOp.getPropertyRef().getPath()), NOT_EQUALS)
        .withValues(Collections.singletonList(NULL));
  }

  private static PropertyQuery simpleLeafOperator(POp pOp) {
    assertHasNChildren(pOp, 0);
    assertHasPathSet(pOp);
    assertHasValueSet(pOp);

    PropertyQuery.QueryOperation operator = SIMPLE_LEAF_OPERATORS.get(pOp.op());
    if (operator == null) throw unsupportedOperation(pOp.op() + " not supported");
    String propertyPath = joinPath(pOp.getPropertyRef().getPath());
    return new PropertyQuery(propertyPath, operator).withValues(List.of(pOp.getValue()));
  }

  private static void assertHasNChildren(POp pOp, int count) {
    List<@NotNull POp> children = pOp.children();
    if (children == null && count == 0) return;
    if (children != null && children.size() == count) return;
    throw unsupportedOperation("Operation must have exactly" + count + "children");
  }

  private static void assertHasAtLeastOneChildren(POp pOp) {
    if (pOp.children() == null || pOp.children().isEmpty())
      throw unsupportedOperation("Operation must have at least one children");
  }

  private static void assertHasPathSet(POp pOp) {
    if (pOp.getPropertyRef() == null
        || pOp.getPropertyRef().getPath() == null
        || pOp.getPropertyRef().getPath().isEmpty())
      throw unsupportedOperation("PropertyRef Path is not present");
  }

  private static void assertHasValueSet(POp pOp) {
    if (pOp.getValue() == null || pOp.getValue().toString().isEmpty())
      throw unsupportedOperation("Value is not present");
  }

  private static RuntimeException unsupportedOperation(String msg) {
    return unchecked(new POpToQueryConversionException(msg));
  }

  private static String joinPath(List<String> strings) {
    return String.join(PATH_SEGMENT_DELIMITER, strings);
  }

  static class POpToQueryConversionException extends UnsupportedOperationException {
    public POpToQueryConversionException(String message) {
      super(message);
    }
  }
}
