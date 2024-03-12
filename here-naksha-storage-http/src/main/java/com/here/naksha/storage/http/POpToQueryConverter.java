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
package com.here.naksha.storage.http;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.models.storage.POpType.*;

import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

public class POpToQueryConverter {

  public static final String EQ_OPERATOR = "=";
  public static final String NOT_EQ_OPERATOR = "!=";
  public static final String NULL = ".null";
  public static final String AND_DELIMITER = "&";

  public static final String OR_DELIMITER = ",";
  public static final String PATH_SEGMENT_DELIMITER = ".";

  private static final Map<POpType, String> SIMPLE_LEAF_OPERATORS = Map.of(
      EQ, EQ_OPERATOR,
      GT, "=gt=",
      GTE, "=gte=",
      LT, "=lt=",
      LTE, "=lte=",
      CONTAINS, "=cs=");

  private POpToQueryConverter() {}

  static String p0pToQuery(POp pOp) {
    if (pOp.op() == AND) return and(pOp);
    else return pOpToMultiValueComparison(pOp).resolve();
  }

  private static String and(POp pOp) {
    assertHasAtLeastOneChildren(pOp);
    return pOp.children().stream().map(POpToQueryConverter::p0pToQuery).collect(Collectors.joining(AND_DELIMITER));
  }

  private static MultiValueComparison pOpToMultiValueComparison(POp pOp) {
    if (pOp.op() == AND) throw unsupportedOperation("AND can be only a top level operation");
    if (pOp.op() == OR) return or(pOp);
    if (pOp.op() == NOT) return not(pOp);
    if (pOp.op() == EXISTS) return exists(pOp);
    return simpleLeafOperator(pOp);
  }

  private static MultiValueComparison or(POp pOp) {
    assertHasAtLeastOneChildren(pOp);

    return pOp.children().stream()
        .map(POpToQueryConverter::pOpToMultiValueComparison)
        .reduce((l, r) -> {
          if (!Objects.equals(l.operator, r.operator))
            throw unsupportedOperation(
                "Operators " + l.operator + " and " + r.operator + " combined in one OR");
          if (!Objects.equals(l.path, r.path)) throw unsupportedOperation("Paths in OR are not equal");
          return new MultiValueComparison(l.operator, l.path, ArrayUtils.addAll(l.values, r.values));
        })
        .orElseThrow(() -> new IllegalStateException("Should not reach here."));
  }

  private static MultiValueComparison not(POp pOp) {
    assertHasNChildren(pOp, 1);
    MultiValueComparison multiValueComparison =
        pOpToMultiValueComparison(pOp.children().get(0));
    String newOperator =
        switch (multiValueComparison.operator) {
          case EQ_OPERATOR -> NOT_EQ_OPERATOR;
          case NOT_EQ_OPERATOR -> EQ_OPERATOR;
          default -> throw unsupportedOperation(multiValueComparison.operator);
        };
    return new MultiValueComparison(newOperator, multiValueComparison.path, multiValueComparison.values);
  }

  private static MultiValueComparison exists(POp pOp) {
    assertHasNChildren(pOp, 0);
    assertHasPathSet(pOp);

    return new MultiValueComparison(NOT_EQ_OPERATOR, pOp.getPropertyRef().getPath(), NULL);
  }

  private static MultiValueComparison simpleLeafOperator(POp pOp) {
    assertHasNChildren(pOp, 0);
    assertHasPathSet(pOp);
    assertHasValueSet(pOp);

    String operator = SIMPLE_LEAF_OPERATORS.get(pOp.op());
    if (operator == null) throw unsupportedOperation(pOp.op() + " not supported");
    return new MultiValueComparison(
        operator, pOp.getPropertyRef().getPath(), pOp.getValue().toString());
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

  static class POpToQueryConversionException extends UnsupportedOperationException {
    public POpToQueryConversionException(String message) {
      super(message);
    }
  }

  private static String translatePathSpecialCases(String path) {
    // f.id in Naksha service is translated to "id" and needs to be translated back
    if (path.equals("id")) return "f.id";
    else return path;
  }

  private record MultiValueComparison(
      @NotNull String operator, @NotNull List<String> path, @NotNull String... values) {

    String resolve() {
      String pathEncoded = encodeAndJoin(PATH_SEGMENT_DELIMITER, path);
      String pathTranslated = translatePathSpecialCases(pathEncoded);
      String valueEncoded = encodeAndJoin(OR_DELIMITER, List.of(values));
      return pathTranslated + operator + valueEncoded;
    }

    private static String encodeAndJoin(String delimiter, List<String> strings) {
      return strings.stream()
          .map(s -> URLEncoder.encode(s, StandardCharsets.UTF_8))
          .collect(Collectors.joining(delimiter));
    }
  }
}
