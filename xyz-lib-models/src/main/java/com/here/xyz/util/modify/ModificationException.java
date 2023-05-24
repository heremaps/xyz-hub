package com.here.xyz.util.modify;

import org.jetbrains.annotations.NotNull;

/**
 * An exception thrown when an error happened while processing a modification.
 */
public class ModificationException extends Exception {

  public ModificationException(@NotNull String msg) {
    super(msg);
  }
}
