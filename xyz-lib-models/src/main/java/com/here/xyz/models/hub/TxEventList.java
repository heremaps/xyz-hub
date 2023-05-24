package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A transaction list persists out of multiple transaction events. Each event represents something that has happened to a collection. This
 * is the container that is used to transport all transactions.
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TxEventList implements Iterable<TxEvent> {
  // TODO: Serialize and deserialize all. Maybe we have to extend a array list and just override get setters!
  //       Or we can teach Jackson to serialize as array from all?
  //       ??? -> @JsonFormat(shape=JsonFormat.Shape.ARRAY)

  /**
   * All transaction items as read from the transaction table. They all need to have the same transaction number.
   */
  private final ArrayList<@NotNull TxEvent> all = new ArrayList<>();

  @JsonIgnore
  private final HashMap<@NotNull String, @NotNull TxEvent> allById = new HashMap<>();

  /**
   * The transaction number, which all events being part of the transaction list share.
   */
  @JsonIgnore
  private @Nullable String txn;

  /**
   * Return the transaction number, which all events being part of the transaction list share, or {@code null}, if the transaction does not
   * have any item.
   *
   * @return The transaction number; {@code null}, if the transaction does not have any item.
   */
  public @Nullable String txn() {
    return txn;
  }

  /**
   * Returns the amount of transaction events being part of this transaction list.
   *
   * @return The amount of transaction events being part of this transaction list.
   */
  public int size() {
    return all.size();
  }

  /**
   * Returns the amount of transaction events that fulfill the given filter.
   *
   * @param filter The filter to apply.
   * @return The amount of transaction events that fulfill the given filter.
   */
  public int count(@NotNull Function<@NotNull TxEvent, @NotNull Boolean> filter) {
    int size = 0;
    for (final @NotNull TxEvent item : all) {
      if (filter.apply(item)) {
        size++;
      }
    }
    return size;
  }

  /**
   * Return filtered transaction event.
   *
   * @param filter The filter to apply.
   * @return A list of all transaction events that match the given filter.
   */
  public @NotNull List<@NotNull TxEvent> get(@NotNull Function<@NotNull TxEvent, @NotNull Boolean> filter) {
    final ArrayList<@NotNull TxEvent> items = new ArrayList<>(4);
    for (final @NotNull TxEvent item : all) {
      if (filter.apply(item)) {
        items.add(item);
      }
    }
    return items;
  }

  /**
   * Return the transaction event with the given identifier.
   *
   * @param id The identifier to query.
   * @return The transaction event with the given identifier; {@code null} if no such transaction event exists.
   */
  public @Nullable TxEvent get(@NotNull String id) {
    return allById.get(id);
  }

  /**
   * Add the given transaction event.
   *
   * @param item The event to add.
   * @return The overridden event, if any.
   * @throws NullPointerException     If the given event is null.
   * @throws IllegalArgumentException If the given event does not have the same transaction number like existing ones.
   */
  public @Nullable TxEvent put(@NotNull TxEvent item) {
    if (txn == null) {
      if (item.txn == null || item.txn.isEmpty()) {
        throw new IllegalArgumentException("The transaction item does have an empty txn");
      }
      txn = item.txn;
    } else if (!txn.equals(item.txn)) {
      throw new IllegalArgumentException(
          "Failed to add transaction item, the given item has txn: '" + item.txn + "', but '" + txn + "' is required!");
    }
    final TxEvent existing = allById.get(item.id);
    if (existing != null) {
      if (existing == item) {
        return existing;
      }
      final boolean removed = all.remove(existing);
      assert removed;
    }
    all.add(item);
    final TxEvent removed = allById.put(item.id, item);
    assert removed == null;
    return existing;
  }

  @Nonnull
  @Override
  public Iterator<TxEvent> iterator() {
    return all.iterator();
  }

  // --------------------------------------------------------------------------------------------------------------------------------
  // -------------------------< Pre-defined filters >--------------------------------------------------------------------------------
  // --------------------------------------------------------------------------------------------------------------------------------

  /**
   * A filter that only returns those transaction events that represent feature modifications.
   *
   * @param event The event to filter.
   * @return true if the event represents a matching transaction event; false otherwise.
   */
  public boolean returnFeatureModification(@NotNull TxEvent event) {
    return event.action == TxAction.MODIFY_FEATURES;
  }

  /**
   * A filter that only returns those transaction events that represent commit messages.
   *
   * @param event The event to filter.
   * @return true if the item represents a matching transaction item; false otherwise.
   */
  public boolean returnCommitMessage(@NotNull TxEvent event) {
    return event.action == TxAction.COMMIT_MESSAGE;
  }

  /**
   * A filter that only returns those transaction events that represent collection modifications.
   *
   * @param event The event to filter.
   * @return true if the item represents a matching transaction event; false otherwise.
   */
  public boolean returnCollectionModification(@NotNull TxEvent event) {
    return event.action == TxAction.CREATE_COLLECTION
        || event.action == TxAction.UPDATE_COLLECTION
        || event.action == TxAction.DELETE_COLLECTION;
  }
}