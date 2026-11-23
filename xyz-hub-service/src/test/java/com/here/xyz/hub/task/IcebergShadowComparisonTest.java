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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.task.IcebergShadowComparison.Outcome.INCOMPARABLE;
import static com.here.xyz.hub.task.IcebergShadowComparison.Outcome.MATCH;
import static com.here.xyz.hub.task.IcebergShadowComparison.Outcome.MISMATCH;
import static com.here.xyz.hub.task.IcebergShadowComparison.Outcome.SKIPPED;
import static org.assertj.core.api.Assertions.assertThat;

import com.here.xyz.hub.task.IcebergShadowComparison.Result;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.SuccessResponse;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class IcebergShadowComparisonTest {

  private static final long LIMIT = 100;

  @Test
  void sameFeaturesInAnotherOrderWithOtherKeyOrderAndNumberFormatsMatch() {
    FeatureCollection primary = fc("{\"id\":\"a\",\"type\":\"Feature\",\"properties\":{\"n\":1,\"m\":\"x\",\"o\":{\"p\":[1,2.5]}}}",
        "{\"id\":\"b\",\"type\":\"Feature\",\"properties\":{}}");
    FeatureCollection iceberg = fc("{\"type\":\"Feature\",\"id\":\"b\",\"properties\":{}}",
        "{\"properties\":{\"o\":{\"p\":[1.0,2.50]},\"m\":\"x\",\"n\":1.0},\"type\":\"Feature\",\"id\":\"a\"}");

    Result result = IcebergShadowComparison.compare(primary, iceberg, LIMIT);

    assertThat(result.outcome()).isEqualTo(MATCH);
    assertThat(result.primaryCount()).isEqualTo(2);
    assertThat(result.icebergCount()).isEqualTo(2);
  }

  @Test
  void emptyResponsesMatch() {
    assertThat(IcebergShadowComparison.compare(fc(), fc(), LIMIT).outcome()).isEqualTo(MATCH);
  }

  @Test
  void missingAndExtraIdsAreListed() {
    Result result = IcebergShadowComparison.compare(fc(feature("a"), feature("b")), fc(feature("b"), feature("c")), LIMIT);

    assertThat(result.outcome()).isEqualTo(MISMATCH);
    assertThat(result.missing()).containsExactly("a");
    assertThat(result.extra()).containsExactly("c");
    assertThat(result.differing()).isEmpty();
  }

  @Test
  void aDifferingFeatureIsListed() {
    Result result = IcebergShadowComparison.compare(fc(feature("a"), "{\"id\":\"b\",\"properties\":{\"n\":1}}"),
        fc(feature("a"), "{\"id\":\"b\",\"properties\":{\"n\":2}}"), LIMIT);

    assertThat(result.outcome()).isEqualTo(MISMATCH);
    assertThat(result.differing()).containsExactly("b");
    assertThat(result.missing()).isEmpty();
    assertThat(result.extra()).isEmpty();
  }

  @Test
  void atMostTenIdsAreListed() {
    String[] features = IntStream.range(0, 15).mapToObj(i -> feature("f" + i)).toArray(String[]::new);

    Result result = IcebergShadowComparison.compare(fc(features), fc(), LIMIT);

    assertThat(result.outcome()).isEqualTo(MISMATCH);
    assertThat(result.missing()).hasSize(IcebergShadowComparison.MAX_LISTED_IDS);
    assertThat(result.reason()).contains("missing=15");
  }

  @Test
  void responsesTruncatedByTheLimitAreIncomparable() {
    Result result = IcebergShadowComparison.compare(fc(feature("a"), feature("b")), fc(feature("a")), 2);

    assertThat(result.outcome()).isEqualTo(INCOMPARABLE);
    assertThat(result.primaryCount()).isEqualTo(2);
  }

  @Test
  void pagedResponsesAreIncomparable() {
    assertThat(IcebergShadowComparison.compare(fc(feature("a")).withNextPageToken("t"), fc(feature("a")), LIMIT).outcome())
        .isEqualTo(INCOMPARABLE);
    assertThat(IcebergShadowComparison.compare(fc(feature("a")), fc(feature("a")).withHandle("ice:1:0:1"), LIMIT).outcome())
        .isEqualTo(INCOMPARABLE);
  }

  @Test
  void responsesLargerThanTheMaximumAreSkipped() {
    FeatureCollection primary = fc(feature("a"), feature("b"));
    int length = com.here.xyz.XyzSerializable.serialize(primary).length();
    assertThat(IcebergShadowComparison.compare(primary, fc(feature("a"), feature("b")), LIMIT, length).outcome()).isEqualTo(MATCH);

    Result result = IcebergShadowComparison.compare(primary, fc(feature("a"), feature("b")), LIMIT, length - 1);
    assertThat(result.outcome()).isEqualTo(SKIPPED);
    assertThat(result.reason()).isEqualTo("too large");
    //Also if only the Iceberg response is too large
    assertThat(IcebergShadowComparison.compare(fc(feature("a")), primary, LIMIT, length - 1).outcome()).isEqualTo(SKIPPED);
  }

  @Test
  void theSizeIsCheckedWithoutComparing() {
    FeatureCollection response = fc(feature("a"), feature("b"));
    int length = com.here.xyz.XyzSerializable.serialize(response).length();
    assertThat(IcebergShadowComparison.isLargerThan(response, length)).isFalse();
    assertThat(IcebergShadowComparison.isLargerThan(response, length - 1)).isTrue();
    assertThat(IcebergShadowComparison.isLargerThan(null, 0)).isFalse();
  }

  @Test
  void duplicateIdsAreIncomparable() {
    assertThat(IcebergShadowComparison.compare(fc(feature("a"), feature("a")), fc(feature("a")), LIMIT).outcome())
        .isEqualTo(INCOMPARABLE);
    assertThat(IcebergShadowComparison.compare(fc("{\"type\":\"Feature\"}"), fc(), LIMIT).outcome()).isEqualTo(INCOMPARABLE);
  }

  @Test
  void responsesOtherThanFeatureCollectionsAreIncomparable() {
    assertThat(IcebergShadowComparison.compare(new SuccessResponse(), fc(), LIMIT).outcome()).isEqualTo(INCOMPARABLE);
    assertThat(IcebergShadowComparison.compare(fc(), null, LIMIT).outcome()).isEqualTo(INCOMPARABLE);
  }

  static FeatureCollection fc(String... features) {
    FeatureCollection fc = new FeatureCollection();
    fc._setFeatures("[" + String.join(",", features) + "]");
    return fc;
  }

  static String feature(String id) {
    return "{\"id\":\"" + id + "\",\"type\":\"Feature\",\"properties\":{\"name\":\"" + id + "\"}}";
  }
}
