package com.here.naksha.lib.core.models.storage;

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({"UseBulkOperation", "ManualArrayToCollectionCopy"})
public class ReadCollections extends ReadRequest<ReadCollections> {

  protected final @NotNull List<@NotNull String> ids = new ArrayList<>();

  public boolean readDeleted() {
    return readDeleted;
  }

  public ReadCollections withReadDeleted(boolean readDeleted) {
    this.readDeleted = readDeleted;
    return self();
  }

  protected boolean readDeleted;

  public @NotNull List<@NotNull String> getIds() {
    return ids;
  }
  public @NotNull ReadCollections withIds(@NotNull String... ids) {
    final List<@NotNull String> this_ids = this.ids;
    for (final String id : ids) {
      this_ids.add(id);
    }
    return self();
  }

  public void setIds(@NotNull List<@NotNull String> ids) {
    final List<@NotNull String> this_ids = this.ids;
    this_ids.addAll(ids);
  }
}