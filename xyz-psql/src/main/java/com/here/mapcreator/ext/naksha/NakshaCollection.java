package com.here.mapcreator.ext.naksha;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

/**
 * A Naksha collection is a physical storage as understood by Naksha-Hub. The same collection can have multiple space identifiers, but it
 * only has one unique consistent collection identifier.
 */
public class NakshaCollection {

  /**
   * Creates a new PSQL collection.
   *
   * @param schema The schema.
   * @param table  The table name, being as well the unique identifier of the collection.
   */
  @JsonCreator
  public NakshaCollection(@JsonProperty @NotNull String schema, @JsonProperty @NotNull String table) {
    this.schema = schema;
    this.table = table;
    this.maxAge = Long.MAX_VALUE;
  }

  /**
   * The schema of the collection.
   */
  @JsonProperty
  public final @NotNull String schema;

  /**
   * The table name, being the collection identifier.
   */
  @JsonProperty
  public final @NotNull String table;

  /**
   * The maximum age of the history entries in days. Zero means no history, {@link Long#MAX_VALUE} means unlimited.
   */
  @JsonProperty
  public long maxAge;
}