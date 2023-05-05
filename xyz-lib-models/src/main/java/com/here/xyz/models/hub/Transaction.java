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
 * A transaction persists out of multiple transaction items. Each item represents something that has happened to a collection. This is the
 * container that is used to transport all transactions.
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Iterable<TxItem> {
  // TODO: Serialize and deserialize all. Maybe we have to extend a array list and just override get setters!
  //       Or we can teach Jackson to serialize as array from all?
  //       ??? -> @JsonFormat(shape=JsonFormat.Shape.ARRAY)

  /**
   * All transaction items as read from the transaction table. They all need to have the same transaction number.
   */
  private final ArrayList<@NotNull TxItem> all = new ArrayList<>();

  @JsonIgnore
  private final HashMap<@NotNull String, @NotNull TxItem> allById = new HashMap<>();

  /**
   * The transaction number.
   */
  @JsonIgnore
  private @Nullable String txn;

  /**
   * Return the transaction number or {@code null}, if the transaction does not have any item.
   *
   * @return The transaction number; {@code null}, if the transaction does not have any item.
   */
  public @Nullable String txn() {
    return txn;
  }

  /**
   * Returns the amount of transaction items being part of this transaction.
   *
   * @return The amount of transaction items being part of this transaction.
   */
  public int size() {
    return all.size();
  }

  /**
   * Returns the amount of transaction items that fulfill the given filter.
   *
   * @param filter The filter to apply.
   * @return The amount of transaction items that fulfill the given filter.
   */
  public int count(@NotNull Function<@NotNull TxItem, @NotNull Boolean> filter) {
    int size = 0;
    for (final @NotNull TxItem item : all) {
      if (filter.apply(item)) {
        size++;
      }
    }
    return size;
  }

  /**
   * Return filtered transaction items.
   *
   * @param filter The filter to apply.
   * @return A list of all transaction items that match the given filter.
   */
  public @NotNull List<@NotNull TxItem> get(@NotNull Function<@NotNull TxItem, @NotNull Boolean> filter) {
    final ArrayList<@NotNull TxItem> items = new ArrayList<>(4);
    for (final @NotNull TxItem item : all) {
      if (filter.apply(item)) {
        items.add(item);
      }
    }
    return items;
  }

  /**
   * Return the transaction item with the given identifier.
   * @param id The identifier to query.
   * @return The transaction with the given identifier; {@code null} if no such transaction item exists.
   */
  public @Nullable TxItem get(@NotNull String id) {
    return allById.get(id);
  }

  /**
   * Add the given transaction item.
   *
   * @param item The item to add.
   * @return The overrides item, if any.
   * @throws NullPointerException     If the given item is null.
   * @throws IllegalArgumentException If the given item does not have the same transaction number as existing ones.
   */
  public @Nullable TxItem put(@NotNull TxItem item) {
    if (txn == null) {
      if (item.txn == null || item.txn.isEmpty()) {
        throw new IllegalArgumentException("The transaction item does have an empty txn");
      }
      txn = item.txn;
    } else if (!txn.equals(item.txn)) {
      throw new IllegalArgumentException("Failed to add transaction item, the given item has txn: '" + item.txn + "', but '" + txn + "' is required!");
    }
    final TxItem existing = allById.get(item.id);
    if (existing != null) {
      if (existing == item) {
        return existing;
      }
      final boolean removed = all.remove(existing);
      assert removed;
    }
    all.add(item);
    final TxItem removed = allById.put(item.id, item);
    assert removed == null;
    return existing;
  }

  @Nonnull
  @Override
  public Iterator<TxItem> iterator() {
    return all.iterator();
  }

  // --------------------------------------------------------------------------------------------------------------------------------
  // --------------------------------------------------------------------------------------------------------------------------------
  // --------------------------------------------------------------------------------------------------------------------------------

  /**
   * A filter that only returns those transaction items that represent feature modifications.
   *
   * @param item The item to filter.
   * @return true if the item represents a matching transaction item; false otherwise.
   */
  public boolean returnFeatureModification(@NotNull TxItem item) {
    return item.action == TxAction.MODIFY_FEATURES;
  }

  /**
   * A filter that only returns those transaction items that represent commit messages.
   *
   * @param item The item to filter.
   * @return true if the item represents a matching transaction item; false otherwise.
   */
  public boolean returnCommitMessage(@NotNull TxItem item) {
    return item.action == TxAction.COMMIT_MESSAGE;
  }

  /**
   * A filter that only returns those transaction items that represent collection modifications.
   *
   * @param item The item to filter.
   * @return true if the item represents a matching transaction item; false otherwise.
   */
  public boolean returnCollectionModification(@NotNull TxItem item) {
    return item.action == TxAction.CREATE_COLLECTION
        || item.action == TxAction.UPDATE_COLLECTION
        || item.action == TxAction.DELETE_COLLECTION;
  }
}