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

package com.here.xyz.connectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.Payload;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.RelocatedEvent;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.service.aws.SimulatedContext;
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
import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings("unused")
public class AbstractConnectorHandlerTest {

  public static final SimulatedContext TEST_CONTEXT = new SimulatedContext("test-function", null);
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
  public void writeDataOut() throws ErrorResponseException, JsonProcessingException {
    TestStorageConnector testStorageConnector = new TestStorageConnector();
    ByteArrayInputStream is = new ByteArrayInputStream(HealthCheckEventString.getBytes());
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    //Convert the event to output stream
    testStorageConnector.handleRequest(is, os, TEST_CONTEXT);

    //Read back the result
    byte[] outputBytes = os.toByteArray();

    XyzResponse response = XyzSerializable.deserialize(outputBytes, XyzResponse.class);
    assertTrue(response instanceof HealthStatus);
  }

  @Test
  public void testWriteLargeDataOut() throws IOException {
    TestStorageConnector testStorageConnector = new TestStorageConnector();
    GetFeaturesByBBoxEvent event = new GetFeaturesByBBoxEvent();
    ByteArrayInputStream is = new ByteArrayInputStream(event.serialize().getBytes());
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    //Convert the event to output stream
    testStorageConnector.handleRequest(is, os, TEST_CONTEXT);

    //Read back the result
    byte[] outputBytes = os.toByteArray();

    InputStream outIs = new ByteArrayInputStream(outputBytes);
    outIs = Payload.prepareInputStream(outIs);

    StringBuilder stringBuilder = new StringBuilder();
    String line;

    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(outIs))) {
      while ((line = bufferedReader.readLine()) != null) {
        stringBuilder.append(line);
      }
    }

    XyzResponse result = XyzSerializable.deserialize(stringBuilder.toString());
    assertNotNull(result);
    assertTrue(result instanceof FeatureCollection);
    assertTrue(!((FeatureCollection) result).getFeatures().isEmpty());
  }

  @Ignore("This is a test for the relocation client. To run it, an S3 bucket and valid credentials are required.")
  @Test
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
  private static class TestStorageConnector extends AbstractConnectorHandler {

    @Override
    public Typed processEvent(Event event) throws JsonProcessingException {
      if (event instanceof HealthCheckEvent)
        return new HealthStatus().withStatus("OK");
      if (event instanceof GetFeaturesByBBoxEvent<?>)
        return generateRandomFeatures(417, 100);
      return null;
    }

    @Override
    protected void initialize(Event event) {}
  }
}
