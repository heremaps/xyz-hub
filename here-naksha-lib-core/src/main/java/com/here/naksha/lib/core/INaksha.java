package com.here.naksha.lib.core;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.features.Extension;
import com.here.naksha.lib.core.models.features.Space;
import com.here.naksha.lib.core.models.features.Storage;
import com.here.naksha.lib.core.models.features.StorageCollection;
import com.here.naksha.lib.core.models.features.Subscription;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.storage.IFeatureReader;
import com.here.naksha.lib.core.storage.IStorage;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The Naksha host interface. When an application bootstraps, it creates a Naksha host implementation and exposes it via the
 * {@link #instance} reference. The reference implementation is based upon the PostgresQL database, but alternative implementations are
 * possible, for example the Naksha extension library will fake a Naksha-Hub.
 */
@SuppressWarnings("unused")
public interface INaksha {

  /**
   * All well-known collections. Still, not all Naksha-Hubs may support them, for example the Naksha extension library currently does not
   * support any collection out of the box!
   */
  final class AdminCollections {

    /**
     * The collections for all spaces.
     */
    public static final StorageCollection SPACES = new StorageCollection("naksha:spaces", 0L);

    /**
     * The collections for all subscriptions.
     */
    public static final StorageCollection SUBSCRIPTIONS = new StorageCollection("naksha:subscriptions", 0L);

    /**
     * The collections for all connectors.
     */
    public static final StorageCollection CONNECTORS = new StorageCollection("naksha:connectors", 0L);

    /**
     * The collections for all storages.
     */
    public static final StorageCollection STORAGES = new StorageCollection("naksha:storages", 0L);
  }

  /**
   * Naksha version constant. The last version compatible with XYZ-Hub.
   */
  String v0_6 = "0.6.0";

  /**
   * Naksha version constant.
   */
  String v2_0_0 = "2.0.0";

  /**
   * Naksha version constant.
   */
  String v2_0_3 = "2.0.3";

  /**
   * The reference to the Naksha implementation provided by the host. Rather use the {@link #get()} method to get the instance.
   */
  AtomicReference<@Nullable INaksha> instance = new AtomicReference<>();

  /**
   * Returns the reference to the Naksha implementation provided by the host.
   *
   * @return the reference to the Naksha implementation provided by the host.
   * @throws NullPointerException if the Naksha interface is not available (no host registered).
   */
  static @NotNull INaksha get() {
    final INaksha hub = instance.getPlain();
    if (hub == null) {
      throw new NullPointerException();
    }
    return hub;
  }

  /**
   * Create a new task for the given event.
   *
   * @param eventClass the class of the event-type to create a task for.
   * @param <EVENT>    the event-type.
   * @return The created task.
   * @throws XyzErrorException If the creation of the task failed for some reason.
   */
  <EVENT extends Event, TASK extends AbstractTask<EVENT>> @NotNull TASK newTask(@NotNull Class<EVENT> eventClass)
      throws XyzErrorException;

  /**
   * Returns the administration storage that is guaranteed to have all the {@link AdminCollections admin collections}. This storage does
   * have the storage number 0.
   *
   * @return the administration storage.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IStorage adminStorage();

  /**
   * Returns the extension with the given extension number.
   *
   * @param number the extension number.
   * @return the extension, if such an extension exists.
   */
  @Nullable
  Extension getExtension(int number);

  /**
   * Returns the cached reader for spaces.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Space> spaceReader();

  /**
   * Returns the cached reader for subscriptions.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Subscription> subscriptionReader();

  /**
   * Returns the cached reader for connectors.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Connector> connectorReader();

  /**
   * Returns the cached reader for storages.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Storage> storageReader();

  /**
   * Returns the cached reader for extensions.
   *
   * @return the reader.
   * @throws UnsupportedOperationException if the operation is not supported.
   */
  @NotNull
  IFeatureReader<Extension> extensionReader();
}
