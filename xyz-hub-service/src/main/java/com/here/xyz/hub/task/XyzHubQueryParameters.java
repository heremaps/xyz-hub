package com.here.xyz.hub.task;

import com.here.xyz.EventTask;
import com.here.xyz.hub.rest.ApiParam.Query;
import io.vertx.core.MultiMap;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Special implementation for query parameters.
 */
public class XyzHubQueryParameters implements MultiMap {

  /**
   * Creates a new query parameter map.
   *
   * @param query the query string as provided by the client.
   */
  public XyzHubQueryParameters(@Nullable String query) {
    // TODO: This implementation is wrong, it only works for one special case, we need to eventually fix it.
    map = MultiMap.caseInsensitiveMultiMap();
    if (query != null && query.length() > 0) {
      final @NotNull String @NotNull [] paramStrings = query.split("&");
      for (final String paramString : paramStrings) {
        final int eqDelimiter = paramString.indexOf("=");
        if (eqDelimiter > 0) {
          final String key = paramString.substring(0, eqDelimiter);
          final boolean decode = Query.TAGS.equals(key);
          final String rawValue = paramString.substring(eqDelimiter + 1);
          if (rawValue.length() > 0) {
            final @NotNull String @NotNull [] values = rawValue.split(",");
            for (@NotNull String value : values) {
              if (decode) {
                try {
                  value = URLDecoder.decode(value, Charset.defaultCharset().name());
                } catch (UnsupportedEncodingException e) {
                  EventTask.currentTask().info("URL decode of query parameter " + key + " failed, reason: " + e.getMessage());
                }
              }
              map.add(key, value);
            }
          }
        }
      }
    }
  }

  private final @NotNull MultiMap map;

  @Override
  public @Nullable String get(@NotNull CharSequence name) {
    return map.get(name);
  }

  @Override
  public @Nullable String get(@NotNull String name) {
    return map.get(name);
  }

  public boolean getBoolean(@NotNull CharSequence name, boolean defaultValue) {
    final String value = map.get(name);
    return "true".equalsIgnoreCase(value) || defaultValue;
  }

  public @Nullable String getString(@NotNull CharSequence name, @Nullable String defaultValue) {
    return map.contains(name) ? map.get(name) : defaultValue;
  }

  @Override
  public @Nullable List<@NotNull String> getAll(@NotNull String name) {
    return map.getAll(name);
  }

  @Override
  public @Nullable List<@NotNull String> getAll(@NotNull CharSequence name) {
    return map.getAll(name);
  }

  @Override
  public boolean contains(@NotNull String name) {
    return map.contains(name);
  }

  @Override
  public boolean contains(@NotNull CharSequence name) {
    return map.contains(name);
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public @NotNull Set<@NotNull String> names() {
    return map.names();
  }

  @Override
  public @NotNull XyzHubQueryParameters add(@NotNull String name, @NotNull String value) {
    map.add(name, value);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters add(@NotNull CharSequence name, @NotNull CharSequence value) {
    map.add(name, value);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters add(@NotNull String name, @NotNull Iterable<@NotNull String> values) {
    map.add(name, values);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters add(@NotNull CharSequence name, @NotNull Iterable<@NotNull CharSequence> values) {
    map.add(name, values);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters addAll(@NotNull MultiMap values) {
    map.addAll(values);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters addAll(@NotNull Map<@NotNull String, @NotNull String> headers) {
    map.addAll(headers);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters set(@NotNull String name, @NotNull String value) {
    map.set(name, value);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters set(@NotNull CharSequence name, @NotNull CharSequence value) {
    map.set(name, value);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters set(@NotNull String name, @NotNull Iterable<@NotNull String> values) {
    map.set(name, values);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters set(@NotNull CharSequence name, @NotNull Iterable<@NotNull CharSequence> values) {
    map.set(name, values);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters setAll(@Nullable MultiMap values) {
    map.setAll(values);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters setAll(@NotNull Map<@NotNull String, @NotNull String> headers) {
    map.setAll(headers);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters remove(@NotNull String name) {
    map.remove(name);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters remove(@NotNull CharSequence name) {
    map.remove(name);
    return this;
  }

  @Override
  public @NotNull XyzHubQueryParameters clear() {
    map.clear();
    return this;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public @NotNull Iterator<@NotNull Entry<@NotNull String, @NotNull String>> iterator() {
    return map.iterator();
  }
}
