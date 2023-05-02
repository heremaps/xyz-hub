package com.here.mapcreator.ext.naksha;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

/**
 * A PostgresQL collection is a set of database tables, which together form a logical collection as understood by Naksha-Hub. The same
 * collection can have multiple space identifiers, but it only has one unique consistent collection identifier.
 */
public class PsqlCollection {

  /**
   * Creates a new PSQL collection.
   *
   * @param id     The unique identifier of the collection, all tables will be prefixed with the identifier.
   * @param schema The schema.
   */
  public PsqlCollection(@NotNull String id, @NotNull String schema) {
    this.id = id;
    this.schema = schema;
    this.headTable = id;
    this.deletedTable = id + "_del";
    this.historyTable = id + "_hst";
  }

  /**
   * The collection identifier.
   */
  @JsonProperty
  public final @NotNull String id;

  /**
   * The schema of the collection.
   */
  @JsonProperty
  public final @NotNull String schema;

  /**
   * The table name of the HEAD table.
   */
  @JsonIgnore
  public final @NotNull String headTable;

  /**
   * The table name of the table storing the deleted features. When a collection is part of a view, then deleted features will override and
   * remove features with the same identifier coming from lower level collections.
   */
  @JsonIgnore
  public final @NotNull String deletedTable;

  /**
   * The table name of the history. In fact the history is a partitioned table where every day has its own partition.
   */
  @JsonIgnore
  public final @NotNull String historyTable;
}