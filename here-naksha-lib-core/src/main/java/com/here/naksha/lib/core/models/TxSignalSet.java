package com.here.naksha.lib.core.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.naksha.lib.core.models.features.TxComment;
import com.here.naksha.lib.core.models.features.TxModifyCollection;
import com.here.naksha.lib.core.models.features.TxModifyFeatures;
import com.here.naksha.lib.core.models.features.TxSignal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A transaction signal set, persists out of multiple transaction signals. Each signal represents something that has happened as part of
 * the transaction. This is just a container that is used to transport all transaction signals.
 */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TxSignalSet implements Iterable<TxSignal> {
  // TODO: Serialize and deserialize all. Maybe we have to extend a array list and just override get
  // setters!
  //       Or we can teach Jackson to serialize as array from all?
  //       ??? -> @JsonFormat(shape=JsonFormat.Shape.ARRAY)

  /**
   * All transaction signals as read from the transaction table. They all must have the same transaction number.
   */
  private final ArrayList<@NotNull TxSignal> all = new ArrayList<>();

  @JsonIgnore
  private final HashMap<@NotNull String, @NotNull TxSignal> allById = new HashMap<>();

  /** The transaction number, which all events being part of the transaction list share. */
  @JsonIgnore
  private @Nullable String txn;

  /**
   * Return the transaction number, which all signals being part of the transaction-set share, or {@code null}, if the set does not have
   * any signal.
   *
   * @return the transaction number; {@code null}, if the transaction is empty.
   */
  public @Nullable String txn() {
    return txn;
  }

  /**
   * Returns the amount of transaction signals being part of this transaction-set.
   *
   * @return The amount of transaction signals being part of this transaction-set.
   */
  public int size() {
    return all.size();
  }

  /**
   * Returns the amount of transaction signals that fulfill the given filter.
   *
   * @param filter the filter to apply.
   * @return the amount of transaction signals that fulfill the given filter.
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
   * Return a filtered transaction signal list.
   *
   * @param filter the filter to apply.
   * @return a list of all transaction signals that match the given filter.
   */
  public @NotNull List<@NotNull TxSignal> get(@NotNull Function<@NotNull TxSignal, @NotNull Boolean> filter) {
    final ArrayList<@NotNull TxSignal> items = new ArrayList<>(4);
    for (final @NotNull TxSignal item : all) {
      if (filter.apply(item)) {
        items.add(item);
      }
    }
    return items;
  }

  /**
   * Return the transaction signal with the given identifier.
   *
   * @param id the identifier to query.
   * @return the transaction signal with the given identifier; {@code null} if no such transaction signal exists.
   */
  public @Nullable TxSignal get(@NotNull String id) {
    return allById.get(id);
  }

  /**
   * Add the given transaction signal.
   *
   * @param signal the signal to add.
   * @return the overridden signal, if any.
   * @throws NullPointerException if the given signal is {@code null} or the {@link TxSignal#txn transaction number} of the signal is
   * {@code null}.
   * @throws IllegalArgumentException if the given signal does not have the same transaction number as the existing ones.
   */
  public @Nullable TxSignal put(@NotNull TxSignal signal) {
    if (txn == null) {
      if (signal.txn.isEmpty()) {
        throw new IllegalArgumentException(
            "The transaction signal does have an empty transaction number (txn)");
      }
      txn = signal.txn;
    } else if (!txn.equals(signal.txn)) {
      throw new IllegalArgumentException("Failed to add transaction signal, the given signal has txn '"
          + signal.txn + "', but '" + txn + "' is required!");
    }
    final TxSignal existing = allById.get(signal.getId());
    if (existing != null) {
      if (existing == signal) {
        return existing;
      }
      final boolean removed = all.remove(existing);
      assert removed;
    }
    all.add(signal);
    final TxSignal removed = allById.put(signal.getId(), signal);
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
   * A filter that only returns those transaction signals that represent feature modifications.
   *
   * @param signal the signal to filter.
   * @return true if the signals represents a matching transaction signals; false otherwise.
   */
  public boolean returnFeatureModification(@NotNull TxSignal signal) {
    return signal instanceof TxModifyFeatures;
  }

  /**
   * A filter that only returns those transaction signals that represent commit messages.
   *
   * @param signal the signal to filter.
   * @return true if the signals represents a matching transaction signals; false otherwise.
   */
  public boolean returnCommitMessage(@NotNull TxSignal signal) {
    return signal instanceof TxComment;
  }

  /**
   * A filter that only returns those transaction signals that represent collection modifications.
   *
   * @param signal the signal to filter.
   * @return true if the signals represents a matching transaction signals; false otherwise.
   */
  public boolean returnCollectionModification(@NotNull TxSignal signal) {
    return signal instanceof TxModifyCollection;
  }
}
