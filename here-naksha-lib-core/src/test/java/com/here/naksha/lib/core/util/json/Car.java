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

public class Car extends Vehicle {

  public static final Car CAR = def(Car.class, "CAR")
      .alias(Car.class, "ANOTHER_CAR")
      .alias(Car.class, 5)
      .alias(Car.class, 5.0);

  public static final Car LONG = def(Car.class, 6);

  public static final Car DOUBLE = def(Car.class, 6.0);

  public static final Car BOOLEAN = def(Car.class, Boolean.TRUE);

  public static final Car NULl = def(Car.class, null);
}
