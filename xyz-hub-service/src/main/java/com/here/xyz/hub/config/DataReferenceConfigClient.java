package com.here.xyz.hub.config;

import com.here.xyz.models.hub.DataReference;
import com.here.xyz.util.service.Initializable;
import io.vertx.core.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class DataReferenceConfigClient implements Initializable {

  protected abstract Future<UUID> doStore(DataReference dataReference);

  protected abstract Future<Optional<DataReference>> doLoad(UUID id);

  protected abstract Future<List<DataReference>> doLoad(
    String entityId,
    Integer startVersion,
    Integer endVersion,
    String contentType,
    String objectType,
    String sourceSystem,
    String targetSystem
  );

  protected abstract Future<Void> doDelete(UUID id);

  public final Future<UUID> store(@Nonnull DataReference dataReference) {
    checkNotNull(dataReference, "DataReference cannot be null");
    return doStore(dataReference);
  }

  public final Future<Optional<DataReference>> load(@Nonnull UUID id) {
    checkNotNull(id, "DataReference id cannot be null");
    return doLoad(id);
  }

  public final Future<List<DataReference>> load(
    @Nonnull String entityId,
    @Nullable Integer startVersion,
    @Nullable Integer endVersion,
    @Nullable String contentType,
    @Nullable String objectType,
    @Nullable String sourceSystem,
    @Nullable String targetSystem
  ) {
    checkNotNull(entityId, "DataReference entityId cannot be null");
    return doLoad(
      entityId,
      startVersion,
      endVersion,
      contentType,
      objectType,
      sourceSystem,
      targetSystem
    );
  }

  public final Future<Void> delete(@Nonnull UUID id) {
    checkNotNull(id, "DataReference id cannot be null");
    return doDelete(id);
  }

}
