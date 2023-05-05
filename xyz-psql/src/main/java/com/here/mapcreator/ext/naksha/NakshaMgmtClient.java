package com.here.mapcreator.ext.naksha;

import com.here.xyz.INaksha;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Subscription;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The Naksha management client using the standard PostgresQL database client. This is a special Naksha client, used to manage spaces,
 * connectors and subscriptions. Normally this is only created and used by the Naksha-Hub itself and exposed to all other parts of the
 * Naksha-Hub via the {@link INaksha#instance}.
 */
public class NakshaMgmtClient extends NakshaPsqlClient<NakshaDataSource> implements INaksha {

  /**
   * The collection for spaces.
   */
  public static final @NotNull String DEFAULT_SPACE_COLLECTION = "naksha:spaces";

  /**
   * The collection for connectors.
   */
  public static final @NotNull String DEFAULT_CONNECTOR_COLLECTION = "naksha:connectors";

  /**
   * The collection for subscriptions.
   */
  public static final @NotNull String DEFAULT_SUBSCRIPTIONS_COLLECTION = "naksha:subscriptions";

  /**
   * Create a new Naksha client instance. You can register this as the main instance by simply setting the {@link INaksha#instance} atomic
   * reference.
   *
   * @param config     The configuration of the database to connect to.
   * @param clientName The name of the Naksha client to be used when opening connections to the Postgres database.
   * @param clientId   The client identification number to use. Except for the main database, normally this number is given by the
   *                   Naksha-Hub as connector number.
   */
  public NakshaMgmtClient(@NotNull PsqlPoolConfig config, @NotNull String clientName, long clientId) {
    super(new NakshaDataSource(PsqlPool.get(config), clientName), clientId);
  }

  // TODO: Create a cache-client for each of the managed collections: spaces, connectors and subscriptions!
  //       This allows us to use the cache client in the future as well for normal feature spaces!

  private final ConcurrentHashMap<@NotNull String, @NotNull Space> allSpacesById = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<@NotNull String, @NotNull Connector> allConnectorsById = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<@NotNull Long, @NotNull Connector> allConnectorsByNumber = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<@NotNull String, @NotNull Subscription> allSubscriptionsById = new ConcurrentHashMap<>();

  @Override
  public @NotNull List<@NotNull Space> getSpacesByCollection(@NotNull String collection) {
    final Enumeration<@NotNull String> keys = allSpacesById.keys();
    final ArrayList<@NotNull Space> spaces = new ArrayList<>();
    while (keys.hasMoreElements()) {
      final String id = keys.nextElement();
      final Space space = allSpacesById.get(id);
      if (space != null && collection.equals(space.getCollection())) {
        spaces.add(space);
      }
    }
    return spaces;
  }

  @Override
  public @Nullable Space getSpaceById(@NotNull String id) {
    return allSpacesById.get(id);
  }

  @Override
  public @Nullable Connector getConnectorById(@NotNull String id) {
    return allConnectorsById.get(id);
  }

  @Override
  public @Nullable Connector getConnectorByNumber(long number) {
    return allConnectorsByNumber.get(number);
  }

  @Override
  public @NotNull Iterable<Connector> getConnectors() {
    return allConnectorsById.values();
  }

  @Override
  public @Nullable Subscription getSubscriptionById(@NotNull String id) {
    return allSubscriptionsById.get(id);
  }
}