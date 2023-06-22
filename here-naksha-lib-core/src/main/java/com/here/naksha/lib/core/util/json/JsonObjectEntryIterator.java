package com.here.naksha.lib.core.util.json;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JsonObjectEntryIterator implements Iterator<@NotNull Entry<@NotNull String, @Nullable Object>> {

  JsonObjectEntryIterator(@NotNull JsonObject object) {
    this.object = object;
    this.fields = object.getJsonClass().fields;
  }

  private final @NotNull JsonObject object;
  private final @NotNull JsonField @NotNull [] fields;
  private int i;
  private @Nullable MapEntry<@NotNull String, @Nullable Object> entry;
  private @Nullable Iterator<@NotNull Entry<@NotNull String, @Nullable Object>> additionalPropertiesIt;

  @NotNull
  MapEntry<@NotNull String, @Nullable Object> entry(@NotNull String key, @Nullable Object value) {
    MapEntry<@NotNull String, @Nullable Object> entry = this.entry;
    if (entry == null) {
      return this.entry = new MapEntry<>(object, key, value);
    }
    entry.key = key;
    entry.value = value;
    return entry;
  }

  @Override
  public boolean hasNext() {
    final @NotNull JsonField[] fields = this.fields;
    while (i < fields.length) {
      if (!object.isUndefined(fields[i])) {
        return true;
      }
      i++;
    }
    if (additionalPropertiesIt == null) {
      additionalPropertiesIt = object.additionalProperties.iterator();
    }
    return additionalPropertiesIt.hasNext();
  }

  @Override
  public Entry<@NotNull String, @Nullable Object> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (i < fields.length) {
      final JsonField field = fields[i++];
      return entry(field.jsonName, field._get(object));
    }
    assert additionalPropertiesIt != null;
    final Entry<@NotNull String, @Nullable Object> entry = additionalPropertiesIt.next();
    return entry(entry.getKey(), entry.getValue());
  }
}
