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

package com.here.xyz.connectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.Payload;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.RelocatedEvent;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("unused")
public class AbstractConnectorHandlerTest {

  private String HealthCheckEventString = "{\"type\":\"HealthCheckEvent\", \"streamId\":\"STREAM_ID_EXAMPLE\"}";

  @SuppressWarnings("SameParameterValue")
  private static FeatureCollection generateRandomFeatures(int featureCount, int propertyCount) throws JsonProcessingException {
    FeatureCollection collection = new FeatureCollection();
    Random random = new Random();

    List<String> pKeys = Stream.generate(() ->
        RandomStringUtils.randomAlphanumeric(10)).limit(propertyCount).collect(Collectors.toList());
    collection.setFeatures(new ArrayList<>());
    collection.getFeatures().addAll(
        Stream.generate(() -> {
          Feature f = new Feature()
              .withGeometry(
                  new Point().withCoordinates(new PointCoordinates(360d * random.nextDouble() - 180d, 180d * random.nextDouble() - 90d)))
              .withProperties(new Properties());
          pKeys.forEach(p -> f.getProperties().put(p, RandomStringUtils.randomAlphanumeric(8)));
          return f;
        }).limit(featureCount).collect(Collectors.toList()));
    return collection;
  }

  @Test
  public void readEvent() {
    InputStream is = new ByteArrayInputStream(HealthCheckEventString.getBytes());
    TestStorageConnector testStorageConnector = new TestStorageConnector();
    try {
      Event<?> event = testStorageConnector.readEvent(is);
      assertTrue(event instanceof HealthCheckEvent);
    } catch (ErrorResponseException e) {
      e.printStackTrace();
      fail("Expected was that a HealthCheckEvent object will be created.");
    }
  }

  @Test
  public void writeDataOut() throws ErrorResponseException {
    TestStorageConnector testStorageConnector = new TestStorageConnector();
    HealthCheckEvent healthCheckEvent = new HealthCheckEvent().withStreamId("TEST_STREAM_ID");
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    // Convert the event to output stream
    testStorageConnector.writeDataOut(os, healthCheckEvent, healthCheckEvent.getIfNoneMatch());

    // Read back the result
    byte[] outputBytes = os.toByteArray();

    Event<?> outputEvent = testStorageConnector.readEvent(new ByteArrayInputStream(outputBytes));
    assertTrue(outputEvent instanceof HealthCheckEvent);
  }

  @Test
  public void testWriteLargeDataOut() throws IOException {
    TestStorageConnector testStorageConnector = new TestStorageConnector();
    FeatureCollection fc = generateRandomFeatures(417, 100);
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    // Convert the event to output stream
    testStorageConnector.writeDataOut(os, fc, null);

    // Read back the result
    byte[] outputBytes = os.toByteArray();

    InputStream is = new ByteArrayInputStream(outputBytes);
    is = Payload.prepareInputStream(is);

    StringBuilder stringBuilder = new StringBuilder();
    String line;

    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is))) {
      while ((line = bufferedReader.readLine()) != null) {
        stringBuilder.append(line);
      }
    }

    FeatureCollection result = XyzSerializable.deserialize(stringBuilder.toString());
    assertNotNull(result);
  }

  //This is a test for the relocation client. To run it, an S3 bucket and valid credentials are required.
  //@Test
  public void testRelocatedEvent() throws Exception {
    RelocationClient client = new RelocationClient("some-s3-bucket-name");
    byte[] bytes = client.relocate("STREAM_ID_EXAMPLE", HealthCheckEventString.getBytes());
    RelocatedEvent relocated = XyzSerializable.deserialize(new ByteArrayInputStream(bytes));

    InputStream input = client.processRelocatedEvent(relocated);
    input = Payload.prepareInputStream(input);
    Event<?> event = XyzSerializable.deserialize(input);
    assertTrue(event instanceof HealthCheckEvent);
  }

  @SuppressWarnings("rawtypes")
  static class TestStorageConnector extends AbstractConnectorHandler {

    @Override
    public Typed processEvent(Event event) {
      return null;
    }

    @Override
    protected void initialize(Event event) {
    }
  }
}
