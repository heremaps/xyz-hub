package com.here.xyz.models.hub;

import static com.here.xyz.models.hub.Storage.STORAGE_ID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.models.geojson.implementation.Feature;
import org.jetbrains.annotations.Nullable;

/**
 * A collection.
 */
@JsonTypeName(value = "StorageCollection")
public class StorageCollection extends Feature {

  // Note: Meta information attached JSON serialized to collection tables in PostgresQL.
  //       COMMENT ON TABLE test IS 'Some table';
  //       SELECT pg_catalog.obj_description('test'::regclass, 'pg_class');
  //       Comments can also be added on other objects, like columns, data types, functions, etc.
  // See:  https://stackoverflow.com/questions/17947274/is-it-possible-to-add-table-metadata-in-postgresql

  public static final String MAX_AGE = "maxAge";
  public static final String HISTORY = "history";
  public static final String DELETED_AT = "deleted";

  /**
   * Create a new empty collection.
   *
   * @param id the identifier of the collection.
   */
  @JsonCreator
  public StorageCollection(@JsonProperty(ID) @Nullable String id) {
    super(id);
  }

  /**
   * The unique storage identifier, being a 40-bit unsigned integer.
   */
  @JsonProperty(STORAGE_ID)
  public long storageId;

  /**
   * The maximum age of the history entries in days. Zero means no history, {@link Long#MAX_VALUE} means unlimited.
   */
  @JsonProperty(MAX_AGE)
  public long maxAge = Long.MAX_VALUE;

  /**
   * Toggle if the history is enabled.
   */
  @JsonProperty(HISTORY)
  public boolean history = Boolean.TRUE;

  /**
   * A value greater than zero implies that the collection shall be treated as deleted and represents the UTC Epoch timestamp in
   * milliseconds when the deletion has been done.
   */
  @JsonProperty(DELETED_AT)
  @JsonInclude(Include.NON_DEFAULT)
  public long deletedAt = 0L;
}