package com.here.naksha.lib.core.util.json;

import static com.fasterxml.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION;
import static com.here.naksha.lib.core.NakshaContext.currentLogger;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.here.naksha.lib.core.view.Deserialize;
import com.here.naksha.lib.core.view.Deserialize.All;
import com.here.naksha.lib.core.view.Serialize;
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
 *
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

  /**
   * If this property set to {@code true}, then all serialization will be done in pretty-print.
   */
  public static boolean DEBUG = false;

  private static class JsonWeakRef extends WeakReference<Json> {

    private JsonWeakRef(@NotNull Json json) {
      super(json);
    }
  }

  /** Create a new Json instance. */
  Json() {
    this.weakRef = new JsonWeakRef(this);
    final JsonFactory jsonFactory = new JsonFactory();
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
    this.serialize = Serialize.All.class;
    this.deserialize = All.class;
  }

  /** The weak reference to this. */
  private final @NotNull JsonWeakRef weakRef;

  /** The reference to the next thread local Json. */
  private @Nullable Json next;

  /** If the same instance opened multiple times. */
  private int openCount;

  /** The Jackson mapper. */
  final @NotNull ObjectMapper mapper;

  /** The serialization view to use. */
  private @NotNull Class<? extends Serialize> serialize;

  /** The deserialization view to use. */
  private @NotNull Class<? extends Deserialize> deserialize;

  /**
   * Return a new dedicated thread local Json parser instance. Can be used recursive.
   *
   * @return The Json instance.
   */
  public static @NotNull Json open() {
    final JsonWeakRef weakRef = idleCache.get();
    Json json = null;
    if (weakRef != null) {
      json = weakRef.get();
    }
    if (json != null) {
      idleCache.set(json.next != null ? json.next.weakRef : null);
    } else {
      json = new Json();
    }
    json.next = current.get();
    current.set(json);
    json.openCount++;
    return json.reset();
  }

  /**
   * If the current thread uses a Json parser instance, acquired by the caller, incrementing the
   * open counter, and returning the same instance. Otherwise, create a new Json parser instance, or
   * reusing an unused one, and return it.
   *
   * @return a Json instance.
   */
  public static @NotNull Json reuse() {
    Json json = current.get();
    if (json != null) {
      json.openCount++;
      return json;
    }
    return open();
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
    if (--openCount <= 0) {
      current.set(next);
      if (openCount < 0) {
        currentLogger().warn("Released a Json instance too often", new IllegalStateException());
      } else {
        final JsonWeakRef weakRef = idleCache.get();
        next = weakRef != null ? weakRef.get() : null;
        idleCache.set(this.weakRef);
      }
    }
  }

  /**
   * Returns the amount of references open to this Json parser instance.
   *
   * @return The amount of references open to this Json parser instance.
   */
  public int openCount() {
    return openCount;
  }

  /** The thread local cache with unused Json instances. */
  private static final ThreadLocal<@Nullable JsonWeakRef> idleCache = new ThreadLocal<>();

  /** The currently used Json instance; if any. */
  private static final ThreadLocal<@Nullable Json> current = new ThreadLocal<>();

  private final HashMap<@NotNull Class<? extends Deserialize>, @NotNull ObjectReader> readers = new HashMap<>();
  private final HashMap<@NotNull Class<? extends Serialize>, @NotNull ObjectWriter> writers = new HashMap<>();
  private final HashMap<@NotNull Class<? extends Serialize>, @NotNull ObjectWriter> pretty_writers = new HashMap<>();

  // ------------------------------------------------------------------------------------------------------------------------------------
  // Standard API
  // ------------------------------------------------------------------------------------------------------------------------------------

  /**
   * Format a string using the {@link Formatter}.
   *
   * @param format The format string.
   * @param args The arguments.
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
  public @NotNull ObjectReader reader(@NotNull Class<? extends Deserialize> view) {
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
  public @NotNull ObjectWriter writer(@NotNull Class<? extends Serialize> view) {
    return writer(view, false);
  }

  /**
   * Returns the writer for the given view.
   *
   * @param view the view to write.
   * @param pretty {@code true} if the writer should pretty print (human-readable); {@code false}
   *     otherwise (compact machine optimized).
   * @return the writer for this view.
   */
  public @NotNull ObjectWriter writer(@NotNull Class<? extends Serialize> view, boolean pretty) {
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
