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

public class Truck extends Vehicle {

  public static final Truck TRUCK = def(Truck.class, "TRUCK").with(Truck.class, (self) -> {
    self.isTruck = true;
  });

  public static final Truck CASE_INSENSITIVE =
      defIgnoreCase(Truck.class, "FooTruck").aliasIgnoreCase(Truck.class, "fOO");

  boolean isTruck;
}
