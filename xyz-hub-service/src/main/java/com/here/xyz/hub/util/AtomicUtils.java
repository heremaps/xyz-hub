/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.hub.util;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicUtils {

    public static boolean compareAndIncrementUpTo(int maxExpect, AtomicInteger i) {
        int currentValue = i.get();
        while (currentValue < maxExpect) {
            if (i.compareAndSet(currentValue, currentValue + 1)) {
                return true;
            }
            currentValue = i.get();
        }
        return false;
    }

    public static boolean compareAndDecrement(int minExpect, AtomicInteger i) {
        int currentValue = i.get();
        while (currentValue > minExpect) {
            if (i.compareAndSet(currentValue, currentValue - 1)) {
                return true;
            }
            currentValue = i.get();
        }
        return false;
    }

}
