package com.here.naksha.lib.psql;


import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.hub.pipelines.Space;
import com.here.naksha.lib.core.models.hub.pipelines.Subscription;
import com.here.naksha.lib.core.models.hub.plugins.Connector;
import com.here.naksha.lib.core.models.hub.plugins.EventHandler;
import com.here.naksha.lib.core.models.hub.plugins.Storage;
import com.here.naksha.lib.core.storage.CollectionCache;
import java.io.IOException;
import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The abstract Naksha-Hub is the base class for the Naksha-Hub implementation, granting access to
 * the administration PostgresQL database. This is a special Naksha client, used to manage spaces,
 * connectors, subscriptions and other administrative content. This client should not be used to
 * query data from a foreign storage, it only holds administrative spaces. Normally this is only
 * created and used by the Naksha-Hub itself and exposed to all other parts of the Naksha-Hub via
 * the {@link INaksha#get()} method.
 */
public abstract class AbstractNakshaHub extends PsqlStorage implements INaksha {

    /** The collection for spaces. */
    public static final @NotNull String DEFAULT_SPACE_COLLECTION = "naksha:spaces";

    /** The collection for connectors. */
    public static final @NotNull String DEFAULT_CONNECTOR_COLLECTION = "naksha:connectors";

    /** The collection for subscriptions. */
    public static final @NotNull String DEFAULT_SUBSCRIPTIONS_COLLECTION = "naksha:subscriptions";

    /**
     * Create a new Naksha client instance and register as default Naksha client.
     *
     * @param config the configuration of the admin-database to connect to.
     * @throws SQLException if any error occurred while accessing the database.
     * @throws IOException if reading the SQL extensions from the resources fail.
     */
    protected AbstractNakshaHub(@NotNull PsqlConfig config) throws SQLException, IOException {
        super(config, 0L);
        instance.getAndSet(this);
    }

    /** Cache. */
    public final @NotNull CollectionCache<Space> spaces = null;

    /** Cache. */
    public final @NotNull CollectionCache<Connector> connectors = null;

    /** Cache. */
    public final @NotNull CollectionCache<Subscription> subscriptions = null;

    /** Cache. */
    public final @NotNull CollectionCache<EventHandler> eventHandlers = null;

    /** Cache. */
    public final @NotNull CollectionCache<Storage> storages = null;

    @Override
    public @Nullable Space getSpaceById(@NotNull String id) {
        return null;
    }

    @Override
    public @Nullable Connector getConnectorById(@NotNull String id) {
        return null;
    }

    @Override
    public @Nullable Storage getStorageById(@NotNull String id) {
        return null;
    }

    @Override
    public @Nullable Storage getStorageByNumber(long number) {
        return null;
    }

    @Override
    public @Nullable EventHandler getExtensionById(@NotNull String id) {
        return null;
    }

    @Override
    public @Nullable Subscription getSubscriptionById(@NotNull String id) {
        return null;
    }

    @Override
    public @NotNull Iterable<Space> iterateSpaces() {
        return null;
    }

    @Override
    public @NotNull Iterable<Connector> iterateConnectors() {
        return null;
    }

    @Override
    public @NotNull Iterable<Storage> iterateStorages() {
        return null;
    }

    @Override
    public @NotNull Iterable<EventHandler> iterateExtensions() {
        return null;
    }

    @Override
    public @NotNull Iterable<Subscription> iterateSubscriptions() {
        return null;
    }
}
