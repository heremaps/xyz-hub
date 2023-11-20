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
package com.here.naksha.lib.core.util.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

class JsonEnumTest {

  @Test
  void testDeserializing() throws JsonProcessingException {
    Vehicle vehicle;
    try (final Json json = Json.get()) {
      vehicle = json.reader().forType(Vehicle.class).readValue("\"CAR\"");
      assertSame(Car.CAR, vehicle);
      assertTrue(vehicle.isDefined());

      // The aliases should return the same!
      vehicle = json.reader().forType(Vehicle.class).readValue("\"ANOTHER_CAR\"");
      assertSame(Car.CAR, vehicle);
      vehicle = json.reader().forType(Vehicle.class).readValue("5");
      assertSame(Car.CAR, vehicle);
      vehicle = json.reader().forType(Vehicle.class).readValue("5.0");
      assertSame(Car.CAR, vehicle);

      vehicle = json.reader().forType(Vehicle.class).readValue("\"TRUCK\"");
      assertSame(Truck.TRUCK, vehicle);
      assertTrue(vehicle.isDefined());

      vehicle = json.reader().forType(Vehicle.class).readValue("\"fooTruCK\"");
      assertSame(Truck.CASE_INSENSITIVE, vehicle);
      assertTrue(vehicle.isDefined());

      vehicle = json.reader().forType(Vehicle.class).readValue("\"FooTruck\"");
      assertSame(Truck.CASE_INSENSITIVE, vehicle);
      assertTrue(vehicle.isDefined());

      vehicle = json.reader().forType(Vehicle.class).readValue("\"Foo\"");
      assertSame(Truck.CASE_INSENSITIVE, vehicle);
      assertTrue(vehicle.isDefined());

      vehicle = json.reader().forType(Vehicle.class).readValue("\"fOo\"");
      assertSame(Truck.CASE_INSENSITIVE, vehicle);
      assertTrue(vehicle.isDefined());

      vehicle = json.reader().forType(Vehicle.class).readValue("\"fOO\"");
      assertSame(Truck.CASE_INSENSITIVE, vehicle);
      assertTrue(vehicle.isDefined());

      vehicle = json.reader().forType(Vehicle.class).readValue("\"UNKNOWN\"");
      assertNotNull(vehicle);
      assertEquals("UNKNOWN", vehicle.toString());
      assertFalse(vehicle.isDefined());
    }
  }

  @Test
  void testSerialization() throws JsonProcessingException {
    String out;
    try (final Json json = Json.get()) {
      out = json.writer().writeValueAsString(Car.CAR);
      assertEquals("\"" + Car.CAR + "\"", out);

      out = json.writer().writeValueAsString(Truck.CASE_INSENSITIVE);
      assertEquals("\"" + Truck.CASE_INSENSITIVE + "\"", out);
      assertEquals("FooTruck", Truck.CASE_INSENSITIVE.toString());

      out = json.writer().writeValueAsString(Car.LONG);
      assertEquals(Car.LONG.toString(), out);

      out = json.writer().writeValueAsString(Car.DOUBLE);
      assertEquals(Car.DOUBLE.toString(), out);

      out = json.writer().writeValueAsString(Car.BOOLEAN);
      assertEquals(Car.BOOLEAN.toString(), out);

      out = json.writer().writeValueAsString(Car.NULl);
      assertEquals(Car.NULl.toString(), out);
    }
  }

  @Test
  void testDoubleDefinition() {
    // We must not be able to define the same value twice in the same namespace.
    assertThrowsExactly(Error.class, () -> {
      JsonEnum.def(Car.class, 5);
    });
  }

  public static class TestObject {
    @JsonProperty
    public Vehicle vehicle;
  }

  @Test
  void testObject() throws JsonProcessingException {
    Object raw;
    TestObject object;
    Truck truck;
    Car car;
    try (final Json jp = Json.get()) {
      raw = jp.reader().forType(TestObject.class).readValue("""
{
"vehicle": "TRUCK"
}""");
      object = assertInstanceOf(TestObject.class, raw);
      truck = assertInstanceOf(Truck.class, object.vehicle);
      assertSame(Truck.TRUCK, truck);

      raw = jp.reader().forType(TestObject.class).readValue("""
{
"vehicle": 5
}""");
      object = assertInstanceOf(TestObject.class, raw);
      car = assertInstanceOf(Car.class, object.vehicle);
      assertSame(Car.CAR, car);
    }
  }
}
