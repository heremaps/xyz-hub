package com.here.xyz.models.hub;

import org.jetbrains.annotations.NotNull;

/**
 * The transaction item type.
 */
public enum TxAction {
  /**
   * The transaction item notifies about that the content of a collection modified.
   */
  MODIFY_FEATURES,

  /**
   * The transaction item notifies about a collection created.
   */
  CREATE_COLLECTION,

  /**
   * The transaction item notifies about a collection modified, for example the meta-data of the collection changed.
   */
  UPDATE_COLLECTION,

  /**
   * The transaction item notifies about a collection deleted.
   */
  DELETE_COLLECTION,

  /**
   * The transaction item represents only a commit message.
   */
  COMMIT_MESSAGE;

  @Override
  public @NotNull String toString() {
    return name();
  }
}
