package com.here.xyz.models.hub.transactions;


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
 * A transaction event list, persists out of multiple transaction events. Each event represents
 * something that has happened as part of the transaction. This is the container that is used to
 * transport all transactions.
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TxSignalSet implements Iterable<TxSignal> {
  // TODO: Serialize and deserialize all. Maybe we have to extend a array list and just override get
  // setters!
  //       Or we can teach Jackson to serialize as array from all?
  //       ??? -> @JsonFormat(shape=JsonFormat.Shape.ARRAY)

  /**
   * All transaction items as read from the transaction table. They all need to have the same
   * transaction number.
   */
  private final ArrayList<@NotNull TxSignal> all = new ArrayList<>();

  @JsonIgnore private final HashMap<@NotNull String, @NotNull TxSignal> allById = new HashMap<>();

  /** The transaction number, which all events being part of the transaction list share. */
  @JsonIgnore private @Nullable String txn;

  /**
   * Return the transaction number, which all events being part of the transaction list share, or
   * {@code null}, if the set does not have any event elements.
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
  public int count(@NotNull Function<@NotNull TxSignal, @NotNull Boolean> filter) {
    int size = 0;
    for (final @NotNull TxSignal item : all) {
      if (filter.apply(item)) {
        size++;
      }
    }
    return size;
  }

  /**
   * Return a filtered transaction event list.
   *
   * @param filter the filter to apply.
   * @return a list of all transaction events that match the given filter.
   */
  public @NotNull List<@NotNull TxSignal> get(
      @NotNull Function<@NotNull TxSignal, @NotNull Boolean> filter) {
    final ArrayList<@NotNull TxSignal> items = new ArrayList<>(4);
    for (final @NotNull TxSignal item : all) {
      if (filter.apply(item)) {
        items.add(item);
      }
    }
    return items;
  }

  /**
   * Return the transaction event with the given identifier.
   *
   * @param id the identifier to query.
   * @return the transaction event with the given identifier; {@code null} if no such transaction
   *     event exists.
   */
  public @Nullable TxSignal get(@NotNull String id) {
    return allById.get(id);
  }

  /**
   * Add the given transaction event.
   *
   * @param event the event to add.
   * @return The overridden event, if any.
   * @throws NullPointerException If the given event is null or the {@link TxSignal#txn txn} of the
   *     event is {@code null}.
   * @throws IllegalArgumentException If the given event does not have the same transaction number
   *     like existing ones.
   */
  public @Nullable TxSignal put(@NotNull TxSignal event) {
    if (txn == null) {
      if (event.txn.isEmpty()) {
        throw new IllegalArgumentException("The transaction item does have an empty txn");
      }
      txn = event.txn;
    } else if (!txn.equals(event.txn)) {
      throw new IllegalArgumentException(
          "Failed to add transaction item, the given item has txn: '"
              + event.txn
              + "', but '"
              + txn
              + "' is required!");
    }
    final TxSignal existing = allById.get(event.getId());
    if (existing != null) {
      if (existing == event) {
        return existing;
      }
      final boolean removed = all.remove(existing);
      assert removed;
    }
    all.add(event);
    final TxSignal removed = allById.put(event.getId(), event);
    assert removed == null;
    return existing;
  }

  @Nonnull
  @Override
  public Iterator<TxSignal> iterator() {
    return all.iterator();
  }

  // --------------------------------------------------------------------------------------------------------------------------------
  // -------------------------< Pre-defined filters
  // >--------------------------------------------------------------------------------
  // --------------------------------------------------------------------------------------------------------------------------------

  /**
   * A filter that only returns those transaction events that represent feature modifications.
   *
   * @param event The event to filter.
   * @return true if the event represents a matching transaction event; false otherwise.
   */
  public boolean returnFeatureModification(@NotNull TxSignal event) {
    return event instanceof TxModifyFeatures;
  }

  /**
   * A filter that only returns those transaction events that represent commit messages.
   *
   * @param event The event to filter.
   * @return true if the item represents a matching transaction item; false otherwise.
   */
  public boolean returnCommitMessage(@NotNull TxSignal event) {
    return event instanceof TxComment;
  }

  /**
   * A filter that only returns those transaction events that represent collection modifications.
   *
   * @param event The event to filter.
   * @return true if the item represents a matching transaction event; false otherwise.
   */
  public boolean returnCollectionModification(@NotNull TxSignal event) {
    return event instanceof TxModifyCollection;
  }
}
