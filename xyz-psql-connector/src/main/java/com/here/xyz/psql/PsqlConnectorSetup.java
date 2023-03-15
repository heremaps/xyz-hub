package com.here.xyz.psql;

import com.here.mapcreator.ext.naksha.NPsqlPool;
import com.here.mapcreator.ext.naksha.NPsqlPoolConfig;
import com.here.mapcreator.ext.naksha.NPsqlSpace;
import com.here.mapcreator.ext.naksha.NPsqlSpaceParams;
import com.here.mapcreator.ext.naksha.NPsqlConnectorParams;
import com.here.xyz.events.Event;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An immutable connector setup that can be used to bind events and processors.
 */
@SuppressWarnings("unused")
public class PsqlConnectorSetup {

  public final @NotNull NPsqlConnectorParams connectorParams;
  public final @NotNull NPsqlPool nakshaPool;
  public final @NotNull NPsqlPool[] nakshaReplicas;
  public final @NotNull NPsqlSpace space;

  public PsqlConnectorSetup(@NotNull Event<?> event, @NotNull String logId) {
    final Map<@NotNull String, @Nullable Object> map = event.getConnectorParams();
    if (map == null) {
      throw new IllegalStateException("Missing connectorParams in event");
    }
    connectorParams = new NPsqlConnectorParams(map, logId);
    nakshaPool = NPsqlPool.get(this.connectorParams.getDbConfig());
    final List<@NotNull NPsqlPoolConfig> replicas = this.connectorParams.getDbReplicas();
    nakshaReplicas = new NPsqlPool[replicas.size()];
    for (int i = 0; i < nakshaReplicas.length; i++) {
      nakshaReplicas[i] = NPsqlPool.get(replicas.get(i));
    }
    final NPsqlSpaceParams spaceParams = NPsqlSpaceParams.of(event.getParams());
    final String spaceId = event.getSpace();
    space = new NPsqlSpace(spaceId, spaceParams.getTableName(spaceId), connectorParams.getSpaceSchema());
  }

  public PsqlConnectorSetup(
      @NotNull NPsqlConnectorParams connectorParams,
      @NotNull String spaceId,
      @Nullable String table,
      @Nullable Map<@NotNull String, @Nullable Object> spaceParams
  ) {
    this.connectorParams = connectorParams;
    nakshaPool = NPsqlPool.get(connectorParams.getDbConfig());
    final List<@NotNull NPsqlPoolConfig> replicas = this.connectorParams.getDbReplicas();
    nakshaReplicas = new NPsqlPool[replicas.size()];
    for (int i = 0; i < nakshaReplicas.length; i++) {
      nakshaReplicas[i] = NPsqlPool.get(replicas.get(i));
    }
    space = new NPsqlSpace(spaceId, table != null ? table : spaceId, connectorParams.getSpaceSchema());
  }

  public @NotNull NPsqlSpace space() {
    return space;
  }

  public @Nullable NPsqlSpace getSpaceById(@Nullable CharSequence spaceId) {
    throw new NotImplementedException("getSpaceById");
  }

  public @Nullable NPsqlSpace getSpaceBySchemaAndTable(@Nullable CharSequence schema, @Nullable CharSequence table) {
    throw new NotImplementedException("getSpaceBySchemaAndTable");
  }

  public boolean isReadOnly() {
    return false;
  }

  public @NotNull NPsqlPool masterPool() {
    if (isReadOnly()) {
      throw new IllegalStateException("This database is read-only");
    }
    return nakshaPool;
  }

  public @NotNull NPsqlPool replicaPool() {
    if (nakshaReplicas.length > 0) {
      return nakshaReplicas[RandomUtils.nextInt(0, nakshaReplicas.length)];
    }
    return nakshaPool;
  }
}
