/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.task;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable.Mappers;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzResponse;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Compares the response of the space's own connector with the response of the Iceberg connector for the same read.
 * Features are matched by ID and compared by a hash of their canonical JSON, which ignores the order of the features and of the
 * object keys and compares numbers by their decimal value. Only one feature is parsed at a time, and a response whose JSON is larger
 * than the given maximum is not compared at all.
 */
public final class IcebergShadowComparison {

  static final int MAX_LISTED_IDS = 10;
  static final String TOO_LARGE = "too large";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HexFormat HEX = HexFormat.of();
  private static final int HASH_BYTES = 16;

  public enum Outcome {
    MATCH,
    MISMATCH,
    INCOMPARABLE,
    /**
     * A response was too large to be compared
     */
    SKIPPED
  }

  /**
   * @param outcome The outcome of the comparison
   * @param reason Why the responses are not comparable or which difference was found, {@code null} for a match
   * @param primaryCount The number of features of the primary response, -1 if unknown
   * @param icebergCount The number of features of the Iceberg response, -1 if unknown
   * @param missing Up to {@value #MAX_LISTED_IDS} IDs of features which only the primary response contains
   * @param extra Up to {@value #MAX_LISTED_IDS} IDs of features which only the Iceberg response contains
   * @param differing Up to {@value #MAX_LISTED_IDS} IDs of features which differ between the responses
   */
  public record Result(Outcome outcome, String reason, int primaryCount, int icebergCount, List<String> missing, List<String> extra,
      List<String> differing) {

    static Result incomparable(String reason, int primaryCount, int icebergCount) {
      return new Result(Outcome.INCOMPARABLE, reason, primaryCount, icebergCount, List.of(), List.of(), List.of());
    }

    static Result skipped(String reason) {
      return new Result(Outcome.SKIPPED, reason, -1, -1, List.of(), List.of(), List.of());
    }
  }

  private IcebergShadowComparison() {}

  /**
   * Compares responses of any size.
   *
   * @see #compare(XyzResponse, XyzResponse, long, long)
   */
  public static Result compare(XyzResponse<?> primary, XyzResponse<?> iceberg, long limit) {
    return compare(primary, iceberg, limit, Long.MAX_VALUE);
  }

  /**
   * @param limit The limit of the read; a response with that many features may be truncated, so it is not compared
   * @param maxChars The maximum length of the JSON of each response; larger responses are {@link Outcome#SKIPPED}
   */
  public static Result compare(XyzResponse<?> primary, XyzResponse<?> iceberg, long limit, long maxChars) {
    if (!(primary instanceof FeatureCollection primaryFc))
      return Result.incomparable("primary response " + typeOf(primary), -1, -1);
    if (!(iceberg instanceof FeatureCollection icebergFc))
      return Result.incomparable("iceberg response " + typeOf(iceberg), -1, -1);
    if (hasToken(primaryFc) || hasToken(icebergFc))
      return Result.incomparable("paged response", -1, -1);

    Map<String, String> primaryById;
    Map<String, String> icebergById;
    try {
      //One response at a time, so only one serialized response is held
      primaryById = hashesById(primaryFc, maxChars);
      icebergById = hashesById(icebergFc, maxChars);
    }
    catch (TooLargeException e) {
      return Result.skipped(TOO_LARGE);
    }
    catch (DuplicateIdException e) {
      return Result.incomparable("duplicate or missing feature IDs", -1, -1);
    }
    catch (Exception e) {
      return Result.incomparable("unreadable response: " + e.getMessage(), -1, -1);
    }
    int primaryCount = primaryById.size();
    int icebergCount = icebergById.size();
    if (limit > 0 && (primaryCount >= limit || icebergCount >= limit))
      return Result.incomparable("limit reached", primaryCount, icebergCount);

    List<String> missing = new ArrayList<>();
    List<String> extra = new ArrayList<>();
    List<String> differing = new ArrayList<>();
    int missingCount = 0, extraCount = 0, differingCount = 0;
    for (Entry<String, String> entry : primaryById.entrySet()) {
      String other = icebergById.get(entry.getKey());
      if (other == null)
        missingCount = add(missing, missingCount, entry.getKey());
      else if (!entry.getValue().equals(other))
        differingCount = add(differing, differingCount, entry.getKey());
    }
    for (String id : icebergById.keySet())
      if (!primaryById.containsKey(id))
        extraCount = add(extra, extraCount, id);

    if (missingCount == 0 && extraCount == 0 && differingCount == 0)
      return new Result(Outcome.MATCH, null, primaryCount, icebergCount, List.of(), List.of(), List.of());
    String reason = "missing=" + missingCount + " extra=" + extraCount + " differing=" + differingCount;
    return new Result(Outcome.MISMATCH, reason, primaryCount, icebergCount, sorted(missing), sorted(extra), sorted(differing));
  }

  private static List<String> sorted(List<String> ids) {
    return ids.stream().sorted().toList();
  }

  private static int add(List<String> ids, int count, String id) {
    if (ids.size() < MAX_LISTED_IDS)
      ids.add(id);
    return count + 1;
  }

  private static boolean hasToken(FeatureCollection fc) {
    return fc.getNextPageToken() != null || fc.getHandle() != null;
  }

  /**
   * @return The hash of each feature by its ID
   */
  private static Map<String, String> hashesById(FeatureCollection fc, long maxChars) throws Exception {
    BoundedWriter json = new BoundedWriter(maxChars);
    try {
      Mappers.DEFAULT_MAPPER.get().writeValue(json, fc);
    }
    catch (Exception e) {
      for (Throwable cause = e; cause != null; cause = cause.getCause())
        if (cause instanceof TooLargeException tooLarge)
          throw tooLarge;
      throw e;
    }

    Map<String, String> hashes = new HashMap<>();
    try (JsonParser parser = MAPPER.createParser(json.toString())) {
      if (parser.nextToken() != JsonToken.START_OBJECT)
        throw new IllegalArgumentException("no object");
      boolean featuresFound = false;
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        String field = parser.currentName();
        JsonToken value = parser.nextToken();
        if (!"features".equals(field)) {
          parser.skipChildren();
          continue;
        }
        if (value != JsonToken.START_ARRAY)
          throw new IllegalArgumentException("no features array");
        featuresFound = true;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          JsonNode feature = parser.readValueAsTree();
          JsonNode id = feature == null ? null : feature.get("id");
          if (id == null || id.isNull() || hashes.put(id.asText(), hash(feature)) != null)
            throw new DuplicateIdException();
        }
      }
      if (!featuresFound)
        throw new IllegalArgumentException("no features array");
    }
    return hashes;
  }

  private static String hash(JsonNode node) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      update(digest, node);
      byte[] hash = digest.digest();
      return HEX.formatHex(hash, 0, HASH_BYTES);
    }
    catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Feeds the canonical form of the node into the digest: object keys sorted, numbers by their decimal value.
   */
  private static void update(MessageDigest digest, JsonNode node) {
    if (node.isObject()) {
      digest.update((byte) '{');
      Map<String, JsonNode> fields = new TreeMap<>();
      for (Iterator<Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
        Entry<String, JsonNode> field = it.next();
        fields.put(field.getKey(), field.getValue());
      }
      fields.forEach((key, value) -> {
        string(digest, key);
        update(digest, value);
      });
      digest.update((byte) '}');
    }
    else if (node.isArray()) {
      digest.update((byte) '[');
      node.forEach(element -> update(digest, element));
      digest.update((byte) ']');
    }
    else if (node.isNumber()) {
      digest.update((byte) 'n');
      string(digest, node.decimalValue().stripTrailingZeros().toPlainString());
    }
    else if (node.isTextual()) {
      digest.update((byte) 's');
      string(digest, node.textValue());
    }
    else if (node.isBoolean())
      digest.update((byte) (node.booleanValue() ? 't' : 'f'));
    else
      digest.update((byte) 'z');
  }

  private static void string(MessageDigest digest, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    digest.update(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
    digest.update((byte) ':');
    digest.update(bytes);
  }

  private static String typeOf(XyzResponse<?> response) {
    return response == null ? "null" : response.getClass().getSimpleName();
  }

  /**
   * Collects the serialized response and stops once it gets longer than the maximum.
   */
  private static final class BoundedWriter extends Writer {

    private final long maxChars;
    private final StringBuilder json = new StringBuilder();

    private BoundedWriter(long maxChars) {
      this.maxChars = maxChars;
    }

    @Override
    public void write(char[] buffer, int offset, int length) throws IOException {
      if (json.length() + (long) length > maxChars)
        throw new TooLargeException();
      json.append(buffer, offset, length);
    }

    @Override
    public void write(String value, int offset, int length) throws IOException {
      if (json.length() + (long) length > maxChars)
        throw new TooLargeException();
      json.append(value, offset, offset + length);
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    @Override
    public String toString() {
      return json.toString();
    }
  }

  private static final class TooLargeException extends IOException {

    private TooLargeException() {
      super(TOO_LARGE);
    }
  }

  private static final class DuplicateIdException extends Exception {

    private DuplicateIdException() {
      super(null, null, false, false);
    }
  }
}
