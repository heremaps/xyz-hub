package com.here.xyz;

import com.here.xyz.events.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Subscription;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The Naksha host interface. When an application bootstraps, it creates a Naksha host implementation and exposes it via the
 * {@link #instance} reference. The reference implementation is based upon the PostgresQL database, but alternative implementations are
 * possible.
 */
@SuppressWarnings("unused")
public interface INaksha {

  /**
   * The reference to the Naksha implementation provided by the host. Rather use the {@link #get()} method to get the instance.
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
   * @param <EVENT>    The event-type.
   * @return The created task.
   * @throws XyzErrorException If the creation of the task failed for some reason.
   */
  <EVENT extends Event, TASK extends AbstractTask<EVENT>> @NotNull TASK newTask(@NotNull Class<EVENT> eventClass) throws XyzErrorException;

  /**
   * Returns a list of all spaces that are directly mapped to the given collection.
   *
   * @param collection The collection identifier to query for.
   * @return The spaces that are mapped to the collection; may be empty.
   */
  @NotNull List<@NotNull Space> getSpacesByCollection(@NotNull String collection);

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
   * Returns the connector with the given number or {@code null}, if no such connector exists.
   *
   * @param number The connector number.
   * @return The connector or {@code null}, if no such connector exists.
   */
  @Nullable Connector getConnectorByNumber(long number);

  /**
   * Return an iterable about all connectors.
   *
   * @return An iterable about all connectors.
   */
  @NotNull Iterable<Connector> getAllConnectors();

  /**
   * Returns the subscription with the given identifier.
   *
   * @param id The identifier.
   * @return The subscription or {@code null}; if no such subscription exists.
   */
  @Nullable Subscription getSubscriptionById(@NotNull String id);

}