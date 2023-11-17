/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.core.util;

import com.here.naksha.lib.core.NakshaVersion;
import java.lang.ref.WeakReference;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A general resource implementation that allows to create resources that can be closed, but are not released until all child resources are
 * closed too. This requires that a public facing is created and given away to the user of the resource. This facade should hold a strong
 * reference to this implementation and akt as a proxy. When the user forgets to call {@link #close()} on the facade, the facade will be
 * garbage collected before this resource receives a {@link #close()} call. In that case the leak-detector running in the background will
 * notice this, print a warning with a stack-trace and then forcefully close the resource.
 *
 * <p>Another advantage of this implementation is, that the user can close the parent resource before the child-resources. This is for
 * example important for the PostgresQL implementation, where the session can be closed, before the results from all open cursors of this
 * connection are done (processed). Using this resource implementation allows exactly this, the user can use the results and close the
 * connection premature. The resources {@link #tryDestruct()} method will not be invoked, until the last child resource is closed. If a
 * resource is leaked, it is auto-closed by the leak detector and this then closes down all parent resources as well.
 *
 * @param <PARENT> The parent type.
 */
@SuppressWarnings("rawtypes")
@AvailableSince(NakshaVersion.v2_0_7)
public abstract class CloseableResource<PARENT extends CloseableResource<?>> {

  private static final Logger log = LoggerFactory.getLogger(CloseableResource.class);

  /**
   * Do not print a leak detected more than ones every "x" milliseconds.
   */
  private static final long PRINT_LEAK_EVERY_X_MILLIS = TimeUnit.SECONDS.toMillis(5);

  /**
   * The timestamp in millis when next to warn for a detected leak.
   */
  private static final AtomicLong logNextLeak = new AtomicLong();

  /**
   * A cache of all created resources. The leak detector will ensure that all resources are closed correctly and that leaks are detected and
   * send to logs as warnings.
   */
  private static final ConcurrentHashMap<CloseableResource, CloseableResource> rootResources =
      new ConcurrentHashMap<>();

  private static void leakDetection() {
    while (true) {
      try {
        Enumeration<CloseableResource> keysEnum = rootResources.keys();
        while (keysEnum.hasMoreElements()) {
          CloseableResource resource = null;
          try {
            resource = keysEnum.nextElement();
            resource.tryDestruct();
          } catch (Throwable t) {
            if (resource != null) {
              log.atError()
                  .setMessage("Fatal error while invoking tryDestruct of resource, stop observing")
                  .setCause(t)
                  .log();
              rootResources.remove(resource, resource);
            }
          }
        }
      } catch (Throwable t) {
        log.error("Unexpected exception while detecting leaks", t);
      }
      try {
        //noinspection BusyWait
        Thread.sleep(500);
      } catch (InterruptedException ignore) {
      }
    }
  }

  static {
    final Thread leakDetector = new Thread(CloseableResource::leakDetection, "CloseableResourceLeakDetector");
    leakDetector.setDaemon(true);
    leakDetector.start();
  }

  /**
   * Create a new resource with the given public facade. This will create a weak-reference to the public facade to detect if the user has
   * forgotten to invoke {@link #close()}. This constructor synchronizes the destruction only within this resource instance.
   *
   * @param proxy  The public facade that holds a strong reference to this implementation and acts as a proxy.
   * @param parent If this resource does have a parent and therefore is a child resource.
   */
  protected CloseableResource(@NotNull Object proxy, @Nullable PARENT parent) {
    this(proxy, parent, new ReentrantLock());
  }

  /**
   * Create a new resource with the given public facade. This will create a weak-reference to the public facade to detect if the user has
   * forgotten to invoke {@link #close()}.
   *
   * @param proxy  The public facade that holds a strong reference to this implementation and acts as a proxy.
   * @param parent If this resource does have a parent and therefore is a child resource.
   * @param mutex  The mutex to synchronize destruction at.
   */
  protected CloseableResource(@NotNull Object proxy, @Nullable PARENT parent, @NotNull ReentrantLock mutex) {
    this.mutex = mutex;
    this.proxy = new WeakReference<>(proxy);
    this.name = proxy.getClass().getName();
    this.allocation = new RuntimeException();
    this.isLeaked = true;
    if (parent == null) {
      // Root resource.
      rootResources.put(this, this);
    } else {
      parent.children.put(this, this);
    }
    this.parent = parent;
  }

  /**
   * The lock that is used to synchronize the destruction.
   */
  protected final @NotNull ReentrantLock mutex;

  /**
   * The reference to the parent PSQL resource (the ones that created this as child).
   */
  private final @Nullable PARENT parent;

  /**
   * Returns the parent; if any.
   *
   * @return the parent; if any.
   */
  protected @Nullable PARENT parent() {
    return parent;
  }

  /**
   * A runtime exception generated when the resource is allocated. It is used as leak detection, so when a resource is not closed and
   * released by the garbage collector. It will hint where the resource was allocated to get a better understanding where the leak may
   * happen.
   */
  private final @NotNull RuntimeException allocation;

  /**
   * The classname of the referent.
   */
  private final @NotNull String name;

  /**
   * The weak reference to the public facade to detect if the user has not invoked {@link #close()}, in that case this implementation is not
   * closed, but the weak reference to the public facade is {@code null}.
   */
  private final @NotNull WeakReference<?> proxy;

  /**
   * All child resources.
   */
  final @NotNull ConcurrentHashMap<CloseableResource, CloseableResource> children = new ConcurrentHashMap<>();

  /**
   * If the resource was intentionally clossed.
   */
  private volatile boolean isClosed;

  /**
   * True if the resource leaked.
   */
  private volatile boolean isLeaked;

  /**
   * Returns the proxy facade.
   *
   * @return the proxy facade; unless it was already garbage collected.
   */
  public final @Nullable Object getProxy() {
    return proxy.get();
  }

  /**
   * Closes this resource intentionally, only called (redirected) from the public facade. If this resource does not have any more living
   * children, then it will be destructed, otherwise destruction is delayed until all children are closed.
   */
  public final void close() {
    mutex.lock();
    try {
      if (!isClosed) {
        isClosed = true;
        isLeaked = false;
        tryDestruct();
      }
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Ensures that this resource is not closed (does not check children).
   *
   * @return this.
   * @throws IllegalStateException If the resource is closed.
   */
  @SuppressWarnings("unchecked")
  public <SELF extends CloseableResource> @NotNull SELF assertNotClosed() {
    if (isClosed()) {
      throw new IllegalStateException("Resource closed");
    }
    return (SELF) this;
  }

  private volatile boolean destructed;

  /**
   * Can be overridden if leaks (garbage collection) should not be logged (it is normal and expected).
   *
   * @return {@code true} if leaks (invocation of {@link #close()} was forgotten) should be logged; {@code false} otherwise.
   */
  protected boolean logLeak() {
    return true;
  }

  /**
   * Tries to destruct this resource. If the resource is already destructed, just returns {@code true} without doing anything. If one child
   * resource is still alive, returns {@code false}. If all child resources are closed, it invokes {@link #destruct()} and then returns
   * {@code true}.
   *
   * @return {@code true} if the resource is destructed; {@code false} if there are still living children.
   */
  private boolean tryDestruct() {
    if (!isClosed()) {
      return false;
    }
    if (destructed) {
      return true;
    }
    final Enumeration<CloseableResource> keyEnum = children.keys();
    while (keyEnum.hasMoreElements()) {
      final CloseableResource child = keyEnum.nextElement();
      if (!child.tryDestruct()) {
        return false;
      }
    }
    assert children.isEmpty();

    // We should not wait forever to prevent deadlocks.
    // It should be realistic that no thread should hold the lock for more than 5 seconds.
    try {
      if (mutex.tryLock(5, TimeUnit.SECONDS)) {
        try {
          // Detect a leak and log warning.
          if (isLeaked && logLeak() && logNextLeak.get() < System.currentTimeMillis()) {
            logNextLeak.set(System.currentTimeMillis() + PRINT_LEAK_EVERY_X_MILLIS);
            log.atWarn()
                .setMessage("Resource leaked")
                .setCause(allocation)
                .log();
          }
          try {
            destruct();
          } catch (Throwable t) {
            log.atError()
                .setMessage("Unexpected exception in destructor of {}")
                .addArgument(getClass().getName())
                .setCause(t)
                .log();
          } finally {
            this.destructed = true;
          }
          final CloseableResource<?> parent = this.parent;
          if (parent != null) {
            parent.children.remove(this, this);
            try {
              parent.tryDestruct();
            } catch (Throwable t) {
              log.warn("Unexpected exception caught when calling parent.tryDestruct", t);
            }
          } else { // Root resource
            rootResources.remove(this, this);
            // root resource is not destructed.
          }
        } finally {
          mutex.unlock();
        }
        return true;
      }
    } catch (Exception e) {
      log.atDebug()
          .setMessage("Unexpected exception while trying to acquire mutex")
          .setCause(e)
          .log();
    }
    return false;
  }

  /**
   * Called to destruct (finalize) the resource. This is called after all children have been destructed before. This basically replaced
   * {@link #finalize()} in a much more reliable way, so normally is called instantly when the last child-resource is closed and do not rely
   * upon the garbage collector. However, when invoking {@link #close()} on the resource or any of its children was forgotten, it falls back
   * to the garbage collector to detect this.
   *
   * <p>The implementation should close all used lower level resources and/or return them to pools (e.g. connection pools). It is
   * guaranteed that this method is only called exactly ones when the resource is really final.
   */
  @SuppressWarnings("removal")
  protected abstract void destruct();

  /**
   * Tests whether the resource is closed. Beware that there could be still child resource being alive.
   *
   * @return {@code true} if the resource is closed; {@code false} otherwise.
   */
  public final boolean isClosed() {
    return isClosed || proxy.get() == null;
  }

  /**
   * Tests whether the resource leaked.
   *
   * @return {@code true} if the resource leaked; {@code false} otherwise.
   */
  public final boolean isLeaked() {
    return isClosed() && isLeaked;
  }

  /**
   * Tests whether the resource has been destructed (finalized). The finalization can be done delayed, so a closed resource does not
   * necessarily be destructed, but a destructed resource is guaranteed to be closed.
   *
   * @return {@code true} if the resource is destructed (finalized); {@code false} otherwise.
   */
  public final boolean isDestructed() {
    return destructed;
  }
}
