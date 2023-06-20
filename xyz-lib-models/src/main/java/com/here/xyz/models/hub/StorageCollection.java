package com.here.xyz.models.hub;

import static com.here.xyz.models.hub.plugins.Storage.NUMBER;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.INaksha;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.hub.plugins.Storage;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A collection is a virtual container for features, managed by a {@link Storage}. All collections
 * optionally have a history and transaction log.
 */
@JsonTypeName(value = "StorageCollection")
@AvailableSince(INaksha.v2_0)
public class StorageCollection extends Feature {

    // Note: Meta information attached JSON serialized to collection tables in PostgresQL.
    //       COMMENT ON TABLE test IS 'Some table';
    //       SELECT pg_catalog.obj_description('test'::regclass, 'pg_class');
    //       Comments can also be added on other objects, like columns, data types, functions, etc.
    // See:
    // https://stackoverflow.com/questions/17947274/is-it-possible-to-add-table-metadata-in-postgresql

    @AvailableSince(INaksha.v2_0)
    public static final String MAX_AGE = "maxAge";

    @AvailableSince(INaksha.v2_0)
    public static final String HISTORY = "history";

    @AvailableSince(INaksha.v2_0)
    public static final String DELETED_AT = "deleted";

    /**
     * Create a new empty collection.
     *
     * @param id the identifier of the collection.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonCreator
    public StorageCollection(@JsonProperty(ID) @NotNull String id) {
        super(id);
    }

    /** The unique storage identifier, being a 40-bit unsigned integer. */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(NUMBER)
    public long storageId;

    /**
     * The maximum age of the history entries in days. Zero means no history, {@link Long#MAX_VALUE}
     * means unlimited.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(MAX_AGE)
    public long maxAge = Long.MAX_VALUE;

    /** Toggle if the history is enabled. */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(HISTORY)
    public boolean history = Boolean.TRUE;

    /**
     * A value greater than zero implies that the collection shall be treated as deleted and
     * represents the UTC Epoch timestamp in milliseconds when the deletion has been done.
     */
    @AvailableSince(INaksha.v2_0)
    @JsonProperty(DELETED_AT)
    @JsonInclude(Include.NON_DEFAULT)
    public long deletedAt = 0L;
}
