package com.here.naksha.lib.core.storage;

public interface IStorageLock extends AutoCloseable {
  @Override
  void close();
}
