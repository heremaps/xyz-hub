package com.here.xyz;


import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.pipelines.Space;
import com.here.xyz.models.hub.pipelines.Subscription;
import com.here.xyz.models.hub.plugins.Connector;
import com.here.xyz.models.hub.plugins.EventHandler;
import com.here.xyz.models.hub.plugins.Storage;
import com.here.xyz.models.payload.Event;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The Naksha host interface. When an application bootstraps, it creates a Naksha host
 * implementation and exposes it via the {@link #instance} reference. The reference implementation
 * is based upon the PostgresQL database, but alternative implementations are possible.
 */
@SuppressWarnings("unused")
public interface INaksha {

    /** Protocol version constant. The last version compatible with XYZ-Hub. */
    String v0_6 = "0.6.0";

    /** Protocol version constant. The first version being incompatible with XYZ-Hub. */
    String v2_0 = "2.0.0";

    /**
     * The reference to the Naksha implementation provided by the host. Rather use the {@link #get()}
     * method to get the instance.
     */
    AtomicReference<@NotNull INaksha> instance = new AtomicReference<>();

    /**
     * Returns the reference to the Naksha implementation provided by the host.
     *
     * @return The reference to the Naksha implementation provided by the host.
     */
    static @NotNull INaksha get() {
        return instance.getPlain();
    }

    /**
     * Create a new task for the given event.
     *
     * @param eventClass The class of the event-type to create a task for.
     * @param <EVENT> The event-type.
     * @return The created task.
     * @throws XyzErrorException If the creation of the task failed for some reason.
     */
    <EVENT extends Event, TASK extends AbstractTask<EVENT>> @NotNull TASK newTask(@NotNull Class<EVENT> eventClass)
            throws XyzErrorException;

    /**
     * Returns the space with the given identifier or {@code null}, if no such space exists.
     *
     * @param id The space identifier.
     * @return The space or {@code null}, if no such space exists.
     */
    @Nullable Space getSpaceById(@NotNull String id);

    /**
     * Returns the connector with the given identifier or {@code null}, if no such connector exists.
     *
     * @param id The connector identifier.
     * @return The connector or {@code null}, if no such connector exists.
     */
    @Nullable Connector getConnectorById(@NotNull String id);

    /**
     * Returns the storage with the given identifier or {@code null}, if no such storage exists.
     *
     * @param id the storage identifier.
     * @return the storage or {@code null}, if no such storage exists.
     */
    @Nullable Storage getStorageById(@NotNull String id);

    /**
     * Returns the storage with the given storage identifier or {@code null}, if no such storage
     * exists.
     *
     * @param number the storage number.
     * @return the storage or {@code null}, if no such storage exists.
     */
    @Nullable Storage getStorageByNumber(long number);

    /**
     * Returns the extension with the given identifier.
     *
     * @param id the identifier.
     * @return the the or {@code null}; if no such the exists.
     */
    @Nullable EventHandler getExtensionById(@NotNull String id);

    /**
     * Returns the subscription with the given identifier.
     *
     * @param id The identifier.
     * @return The subscription or {@code null}; if no such subscription exists.
     */
    @Nullable Subscription getSubscriptionById(@NotNull String id);

    /**
     * Return an iterable about all spaces.
     *
     * @return An iterable about all spaces.
     */
    @NotNull Iterable<Space> iterateSpaces();

    /**
     * Return an iterable about all connectors.
     *
     * @return An iterable about all connectors.
     */
    @NotNull Iterable<Connector> iterateConnectors();

    /**
     * Return an iterable about all storages.
     *
     * @return An iterable about all storages.
     */
    @NotNull Iterable<Storage> iterateStorages();

    /**
     * Return an iterable about all extensions.
     *
     * @return An iterable about all extensions.
     */
    @NotNull Iterable<EventHandler> iterateExtensions();

    /**
     * Return an iterable about all subscriptions.
     *
     * @return An iterable about all subscriptions.
     */
    @NotNull Iterable<Subscription> iterateSubscriptions();
}
