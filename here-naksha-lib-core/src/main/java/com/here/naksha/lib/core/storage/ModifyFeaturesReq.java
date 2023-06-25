package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public record ModifyFeaturesReq<FEATURE extends Feature>(
    @NotNull List<@NotNull FEATURE> insert,
    @NotNull List<@NotNull FEATURE> update,
    @NotNull List<@NotNull FEATURE> upsert,
    @NotNull List<@NotNull DeleteOp> delete) {
  public ModifyFeaturesReq() {
    this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
  }
}
