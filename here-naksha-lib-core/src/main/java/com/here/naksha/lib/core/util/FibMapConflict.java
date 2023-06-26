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
package com.here.naksha.lib.core.util;

import com.here.naksha.lib.core.lambdas.F1;
import com.here.naksha.lib.core.lambdas.F4;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The object to return, when a {@link FibMap#put(Object, Object, Object, boolean, Object[])} fails,
 * because the current value is not the expected. The constructor ({@code FibMapConflict::new}) can
 * be passed as last argument to {@link FibMap#put(Object, Object, Object, boolean, Object[], F1,
 * F4)}.
 *
 * @param key The key, that should be modified.
 * @param expected_value The value expected.
 * @param new_value The value to be set.
 * @param value The value found (will not the expected).
 */
public record FibMapConflict(
    @NotNull Object key, @Nullable Object expected_value, @Nullable Object new_value, @Nullable Object value) {}
