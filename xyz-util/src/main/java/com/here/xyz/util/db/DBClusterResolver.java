/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.util.db;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.Name;
import org.xbill.DNS.SimpleResolver;

public class DBClusterResolver {
  private static final Pattern RDS_CLUSTER_HOSTNAME_PATTERN = Pattern.compile("(.+).cluster-.*.rds.amazonaws.com.*");
  private static final int DNS_LOOKUP_TIMEOUT_SECONDS = 2;

  public static String getClusterIdFromHostname(String hostname) {
    if(hostname == null) return null;
    return Optional.ofNullable(extractClusterId(hostname)).orElse(resolveAndExtractClusterId(hostname));
  }

  private static String extractClusterId(String url) {
    Matcher matcher = RDS_CLUSTER_HOSTNAME_PATTERN.matcher(url);
    return matcher.matches() ? matcher.group(1) : null;
  }

  private static String resolveAndExtractClusterId(String hostname) {
    try {
      SimpleResolver resolver = new SimpleResolver();
      resolver.setTimeout(Duration.of(DNS_LOOKUP_TIMEOUT_SECONDS, ChronoUnit.SECONDS));
      Lookup lookup = new Lookup(hostname);
      lookup.setResolver(resolver);

      List<String> records = Arrays.stream(lookup.run()).map(Record::toString).collect(Collectors.toList());
      records.addAll(Arrays.stream(lookup.getAliases()).map(Name::toString).collect(Collectors.toList()));

      for(String record : records) {
        String clusterId = extractClusterId(record);
        if(clusterId != null) return clusterId;
      }
    } catch (Exception e) {
      // Do nothing
    }
    return null;
  }
}
