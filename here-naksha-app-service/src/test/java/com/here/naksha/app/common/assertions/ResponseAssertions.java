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
package com.here.naksha.app.common.assertions;

import static com.here.naksha.app.common.TestUtil.parseJson;
import static com.here.naksha.app.service.http.NakshaHttpHeaders.STREAM_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.app.common.TestUtil;
import com.here.naksha.app.service.models.FeatureCollectionRequest;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.XyzReference;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.ArraySizeComparator;

public class ResponseAssertions {

  private final HttpResponse<String> subject;
  private XyzFeatureCollection collectionResponse;

  private ResponseAssertions(HttpResponse<String> subject) {
    this.subject = subject;
  }

  public static ResponseAssertions assertThat(HttpResponse<String> response) {
    Assertions.assertNotNull(response, "Can't run assertions on null HttpResponse");
    return new ResponseAssertions(response);
  }

  public ResponseAssertions hasStatus(int expectedStatus) {
    Assertions.assertEquals(expectedStatus, subject.statusCode(), "Response status mismatch");
    return this;
  }

  public ResponseAssertions hasStreamIdHeader(String expectedStreamId) {
    return hasHeader(STREAM_ID, expectedStreamId);
  }

  public ResponseAssertions hasHeader(String key, String expectedValue) {
    Optional<String> headerVal = subject.headers().firstValue(key);
    headerVal.ifPresentOrElse(
        headerValue -> Assertions.assertEquals(expectedValue, headerValue),
        () -> Assertions.fail("Response does not have header with key: " + key));
    return this;
  }

  public ResponseAssertions hasJsonBodyFromFile(String testFilePath) {
    return hasJsonBody(TestUtil.loadFileOrFail(testFilePath));
  }

  public ResponseAssertions hasJsonBody(String expectedJsonBody) {
    return hasJsonBody(expectedJsonBody, "Actual and expected json body don't match");
  }

  public ResponseAssertions hasJsonBody(String expectedJsonBody, String failureMessage) {
    String actualBody = subject.body();
    Assertions.assertNotNull(actualBody, "Response body is null");
    try {
      JSONAssert.assertEquals(failureMessage, expectedJsonBody, actualBody, JSONCompareMode.LENIENT);
    } catch (JSONException e) {
      Assertions.fail("Unable to parse response body", e);
    }
    return this;
  }

  public ResponseAssertions hasInsertedCountMatchingWithFeaturesInRequest(final @NotNull String reqBody) throws JSONException {
    final FeatureCollectionRequest collectionRequest = parseJson(reqBody, FeatureCollectionRequest.class);
    return hasMatchingInsertedCount(collectionRequest.getFeatures().size());
  }

  public ResponseAssertions hasMatchingInsertedCount(int cnt) throws JSONException {
    JSONAssert.assertEquals("{inserted:[" + cnt + "]}", subject.body(),
        new ArraySizeComparator(JSONCompareMode.LENIENT));
    return this;
  }

  public ResponseAssertions hasInsertedIdsMatchingFeatureIds(final @Nullable String prefixId) {
    if (collectionResponse == null) {
      collectionResponse = parseJson(subject.body(), XyzFeatureCollection.class);
    }
    final List<String> insertedIds = collectionResponse.getInserted();
    final List<XyzFeature> features = collectionResponse.getFeatures();
    for (int i = 0; i < insertedIds.size(); i++) {
      if (prefixId != null) {
        assertTrue(
            insertedIds.get(i).startsWith(prefixId),
            "Inserted Feature Id in the response doesn't start with given prefix Id : " + prefixId);
      }
      assertEquals(
          insertedIds.get(i),
          features.get(i).getId(),
          "Mismatch between inserted v/s feature ID in the response at idx : " + i);
    }
    return this;
  }

  public ResponseAssertions hasMatchingUpdatedCount(int cnt) throws JSONException {
    JSONAssert.assertEquals("{updated:[" + cnt + "]}", subject.body(),
        new ArraySizeComparator(JSONCompareMode.LENIENT));
    return this;
  }

  public ResponseAssertions hasUpdatedIdsMatchingFeatureIds(final @Nullable String prefixId) {
    if (collectionResponse == null) {
      collectionResponse = parseJson(subject.body(), XyzFeatureCollection.class);
    }
    final List<String> updatedIds = collectionResponse.getUpdated();
    final List<XyzFeature> features = collectionResponse.getFeatures();
    for (int i = 0; i < updatedIds.size(); i++) {
      if (prefixId != null) {
        assertTrue(
            updatedIds.get(i).startsWith(prefixId),
            "Updated Feature Id in the response doesn't start with given prefix Id : " + prefixId);
      }
      assertEquals(
          updatedIds.get(i),
          features.get(i).getId(),
          "Mismatch between updated v/s feature ID in the response at idx : " + i);
    }
    return this;
  }

  public ResponseAssertions hasFeatureReferencedByViolations(int featureIdx, int[] violationIndices) {
    if (collectionResponse == null) {
      collectionResponse = parseJson(subject.body(), XyzFeatureCollection.class);
    }
    final List<XyzFeature> features = collectionResponse.getFeatures();
    final List<XyzFeature> violations = collectionResponse.getViolations();
    // obtain feature Id
    final String fId = features.get(featureIdx).getId();
    assertNotNull(fId, "Feature Id at index " + featureIdx + " found null");
    // match feature Id with references of Violation object at given index positions
    assertNotNull(violations, "No violations found in response");
    for (int i = 0; i < violationIndices.length; i++) {
      int vIdx = violationIndices[i];
      final List<XyzReference> references = violations.get(vIdx).getProperties().getReferences();
      assertNotNull(references, "References missing for violation at idx " + vIdx);
      for (final XyzReference reference : references) {
        assertNotNull(reference.getId(), "Id missing in references for violation at idx " + vIdx);
        assertEquals(fId, reference.getId(), "Referenced featured id doesn't match for Violation at idx " + vIdx);
      }
    }
    return this;
  }


  public ResponseAssertions hasUuids() {
    if (collectionResponse == null) {
      collectionResponse = parseJson(subject.body(), XyzFeatureCollection.class);
    }
    final List<XyzFeature> features = collectionResponse.getFeatures();
    for (final XyzFeature feature : features) {
      assertNotNull(
          feature.getProperties().getXyzNamespace().getUuid(),
          "UUID found missing in response for feature id " + feature.getId());
    }
    return this;
  }

  public ResponseAssertions hasNoViolations() {
    if (collectionResponse == null) {
      collectionResponse = parseJson(subject.body(), XyzFeatureCollection.class);
    }
    assertNull(collectionResponse.getViolations(), "No violations were expected");
    return this;
  }

  public ResponseAssertions hasNoNextPageToken() {
    if (collectionResponse==null) collectionResponse = parseJson(subject.body(), XyzFeatureCollection.class);
    assertNull(collectionResponse.getNextPageToken(), "No nextPageToken was expected");
    return this;
  }

}
