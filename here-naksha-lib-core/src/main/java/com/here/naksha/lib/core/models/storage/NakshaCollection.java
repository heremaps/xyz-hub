package com.here.naksha.lib.core.models.storage;

import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import org.jetbrains.annotations.Nullable;

public class NakshaCollection extends NakshaFeature {

  /**
   * Create a new empty feature.
   *
   * @param id The ID; if {@code null}, then a random one is generated.
   */
  public NakshaCollection(@Nullable String id) {
    super(id);
  }
}
