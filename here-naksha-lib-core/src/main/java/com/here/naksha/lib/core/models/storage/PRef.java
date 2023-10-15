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
package com.here.naksha.lib.core.models.storage;

import static com.here.naksha.lib.core.models.storage.PRef.Constants.*;

import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PRef {

  public PRef(@NotNull String... path) {
    this.propertyPath = List.of(path);
  }

  private @NotNull PRef withTagName(@NotNull String tagName) {
    this.tagName = tagName;
    return this;
  }

  public static class Constants {

    public static final PRef ID = new PRef("id");
    public static final PRef APP_ID = new PRef("properties", "@ns:com:here:xyz", "app_id");
    public static final PRef AUTHOR = new PRef("properties", "@ns:com:here:xyz", "author");
    public static final PRef UUID = new PRef("properties", "@ns:com:here:xyz", "uuid");
    public static final PRef MRID = new PRef("properties", "@ns:com:here:xyz", "mrid");
    public static final PRef QRID = new PRef("properties", "@ns:com:here:xyz", "qrid");
    public static final PRef TXN = new PRef("properties", "@ns:com:here:xyz", "txn");
    public static final PRef TXN_NEXT = new PRef("properties", "@ns:com:here:xyz", "txn_next");
  }

  private @Nullable String tagName;

  private final @NotNull List<@NotNull String> propertyPath;

  public @NotNull List<@NotNull String> propertyPath() {
    return propertyPath;
  }

  public @Nullable String tagName() {
    return tagName;
  }

  public static @NotNull PRef id() {
    return ID;
  }

  public static @NotNull PRef app_id() {
    return APP_ID;
  }

  public static @NotNull PRef author() {
    return AUTHOR;
  }

  public static @NotNull PRef mrid() {
    return MRID;
  }

  public static @NotNull PRef qrid() {
    return QRID;
  }

  public static @NotNull PRef uuid() {
    return UUID;
  }

  public static @NotNull PRef tag(@NotNull String name) {
    return new PRef(name, "properties", "@ns:com:here:xyz", "tags").withTagName(name);
  }

  public static @NotNull PRef txn() {
    return TXN;
  }

  public static @NotNull PRef txn_next() {
    return TXN_NEXT;
  }
}
