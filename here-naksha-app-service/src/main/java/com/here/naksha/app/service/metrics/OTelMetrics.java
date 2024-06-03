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
package com.here.naksha.app.service.metrics;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OTelMetrics {

  private static final Logger logger = LoggerFactory.getLogger(OTelMetrics.class);
  private static final Meter meter = GlobalOpenTelemetry.meterBuilder("io.opentelemetry.metrics.memory")
      .setInstrumentationVersion("1.28.0") // as per otel.version in pom.xml
      .build();

  public static void init() {
    // This will keep collecting memory utilization in background
    meter.gaugeBuilder("mem_used_pct")
        .setDescription("Heap-Memory used percentage")
        .setUnit("percent")
        .buildWithCallback((r) -> {
          Runtime rt = Runtime.getRuntime();
          long max = rt.maxMemory();
          long total = rt.totalMemory();
          long free = rt.freeMemory();
          long used = total - free;
          double usedPct = ((double) used / max) * 100.00;
          BigDecimal bd = new BigDecimal(usedPct).setScale(2, RoundingMode.HALF_EVEN);
          r.record(bd.doubleValue());
        });
  }
}
