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

import static com.fasterxml.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactory.Feature;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.core.view.ViewSerialize;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import java.lang.ref.WeakReference;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Locale;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Thread local JSON handling. To be used like:
 *
 * <pre>{@code
 * try (var json = newJson()) {
 *   var result = json.deserialize(input, type, view);
 * }
 * }</pre>
 * <p>
 * If you want to use the current thread local instance, for whatever reason, do:
 *
 * <pre>{@code
 * try (var json = currentJson()) {
 *   var result = json.deserialize(input, type, view);
 * }
 * }</pre>
 *
 * <p><b>Note</b>: Using the current thread local instance can cause side effects.
 */
@SuppressWarnings("unused")
public final class Json implements AutoCloseable {

  private static final String[] ESCAPE = new String[] {
    "\\u0000", // null
    "\\u0001", // start of heading
    "\\u0002", // start of text
    "\\u0003", // end of text
    "\\u0004", // end of transmission
    "\\u0005", // enquiry
    "\\u0006", // ack
    "\\u0007", // bell
    "\\u0008", // backspace
    "\\t", // tab
    "\\n", // linefeed
    "\\u000b", // vertical tab
    "\\u000c", // form feed (new page)
    "\\r", // carriage return
    "\\u000e", // shift out
    "\\u000f", // shift in
    "\\u0010", // data link escape
    "\\u0011", // device control 1
    "\\u0012", // device control 2
    "\\u0013", // device control 3
    "\\u0014", // device control 4
    "\\u0015", // negative acknowledge
    "\\u0016", // idle
    "\\u0017", // end of transaction block
    "\\u0018", // cancel
    "\\u0019", // end of medium
    "\\u001a", // substitute
    "\\u001b", // escape
    "\\u001c", // file separator
    "\\u001d", // group separator
    "\\u001e", // record separator
    "\\u001f" // unit separator
  };

  /**
   * Escapes a string into JSON encoding.
   *
   * @param text the text to be escaped.
   * @param sb   the string-builder into which to add the escaped string; if {@code null}, then a new string builder is created.
   * @return the string builder with the escaped string.
   */
  public static @NotNull StringBuilder toJsonString(final @NotNull CharSequence text, @Nullable StringBuilder sb) {
    if (sb == null) {
      sb = new StringBuilder();
    }
    final int length = text.length();
    sb.append('"');
    for (int i = 0; i < length; i++) {
      final char c = text.charAt(i);
      if (c == '"') {
        sb.append('\\').append('"');
      } else if (c == '\\') {
        sb.append('\\').append('\\');
      } else if (c < ESCAPE.length) {
        sb.append(ESCAPE[c]);
      } else {
        sb.append(c);
      }
    }
    sb.append('"');
    return sb;
  }

  /**
   * If this property set to {@code true}, then all serialization will be done in pretty-print.
   */
  public static boolean DEBUG = false;

  private static class JsonWeakRef extends WeakReference<Json> {

    private JsonWeakRef(@NotNull Json json) {
      super(json);
    }
  }

  /**
   * Create a new Json instance.
   */
  Json() {
    this.weakRef = new JsonWeakRef(this);
    final JsonFactory jsonFactory = new JsonFactoryBuilder()
        .configure(Feature.INTERN_FIELD_NAMES, false)
        .configure(Feature.CANONICALIZE_FIELD_NAMES, false)
        .configure(Feature.USE_THREAD_LOCAL_FOR_BUFFER_RECYCLING, true)
        .build();
    jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    this.mapper = JsonMapper.builder(jsonFactory)
        .enable(DEFAULT_VIEW_INCLUSION) // , SORT_PROPERTIES_ALPHABETICALLY
        .serializationInclusion(Include.NON_NULL)
        .visibility(PropertyAccessor.SETTER, Visibility.ANY)
        .visibility(PropertyAccessor.GETTER, Visibility.PUBLIC_ONLY)
        .visibility(PropertyAccessor.IS_GETTER, Visibility.NONE)
        .visibility(PropertyAccessor.CREATOR, Visibility.ANY)
        .configure(SerializationFeature.CLOSE_CLOSEABLE, false)
        .addModule(new JsonModule())
        .build();
    this.wkbReader = new WKBReader(new GeometryFactory(new PrecisionModel(), 4326));
    this.wkbWriter = new WKBWriter(3);
  }

  /**
   * The WKB reader for PostgresQL.
   */
  public final @NotNull WKBReader wkbReader;

  /**
   * The WKB writer for PostgresQL.
   */
  public final @NotNull WKBWriter wkbWriter;

  /**
   * The weak reference to this.
   */
  private final @NotNull JsonWeakRef weakRef;

  /**
   * The reference to the next thread local Json.
   */
  private @Nullable Json next;

  /**
   * The Jackson mapper.
   */
  final @NotNull ObjectMapper mapper;

  /**
   * Return a new dedicated thread local Json parser instance. Can be used recursive.
   *
   * @return The Json instance.
   */
  public static @NotNull Json get() {
    final JsonWeakRef weakRef = idleCache.get();
    Json json = null;
    if (weakRef != null) {
      json = weakRef.get();
    }
    if (json != null) {
      idleCache.set(json.next != null ? json.next.weakRef : null);
      return json.reset();
    }
    return new Json();
  }

  /**
   * Reset this instance into the default state.
   *
   * @return this.
   */
  public @NotNull Json reset() {
    return this;
  }

  @Override
  public void close() {
    final JsonWeakRef weakRef = idleCache.get();
    next = weakRef != null ? weakRef.get() : null;
    idleCache.set(this.weakRef);
  }

  /**
   * The thread local cache with unused Json instances.
   */
  private static final ThreadLocal<@Nullable JsonWeakRef> idleCache = new ThreadLocal<>();

  private final HashMap<@NotNull Class<? extends ViewDeserialize>, @NotNull ObjectReader> readers = new HashMap<>();
  private final HashMap<@NotNull Class<? extends ViewSerialize>, @NotNull ObjectWriter> writers = new HashMap<>();
  private final HashMap<@NotNull Class<? extends ViewSerialize>, @NotNull ObjectWriter> pretty_writers =
      new HashMap<>();

  // ------------------------------------------------------------------------------------------------------------------------------------
  // Standard API
  // ------------------------------------------------------------------------------------------------------------------------------------

  /**
   * Format a string using the {@link Formatter}.
   *
   * @param format The format string.
   * @param args   The arguments.
   * @return The formatted string.
   */
  public @NotNull String format(@NotNull String format, Object... args) {
    return String.format(Locale.US, format, args);
  }

  /**
   * Returns the reader for the given view.
   *
   * @param view The view to read.
   * @return The reader for this view.
   */
  public @NotNull ObjectReader reader(@NotNull Class<? extends ViewDeserialize> view) {
    ObjectReader reader = readers.get(view);
    if (reader == null) {
      reader = mapper.readerWithView(view);
      readers.put(view, reader);
    }
    return reader;
  }

  /**
   * Returns the writer for the given view.
   *
   * @param view the view to write.
   * @return the writer for this view.
   */
  public @NotNull ObjectWriter writer(@NotNull Class<? extends ViewSerialize> view) {
    return writer(view, false);
  }

  /**
   * Returns the writer for the given view.
   *
   * @param view   the view to write.
   * @param pretty {@code true} if the writer should pretty print (human-readable); {@code false} otherwise (compact machine optimized).
   * @return the writer for this view.
   */
  public @NotNull ObjectWriter writer(@NotNull Class<? extends ViewSerialize> view, boolean pretty) {
    ObjectWriter writer;
    if (DEBUG || pretty) {
      writer = pretty_writers.get(view);
      if (writer == null) {
        writer = mapper.writerWithView(view).withDefaultPrettyPrinter();
        pretty_writers.put(view, writer);
      }
    } else {
      writer = writers.get(view);
      if (writer == null) {
        writer = mapper.writerWithView(view);
        writers.put(view, writer);
      }
    }
    return writer;
  }
}
