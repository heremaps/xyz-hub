package com.here.naksha.app.init.context;

import static com.here.naksha.app.init.context.TestContext.State.NOT_STARTED;
import static com.here.naksha.app.init.context.TestContext.State.STARTED;
import static com.here.naksha.app.init.context.TestContext.State.STOPPED;
import static com.here.naksha.app.init.context.TestContext.State.STOPPING;

import com.here.naksha.app.service.NakshaApp;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestContext {

  private static final Logger log = LoggerFactory.getLogger(TestContext.class);

  private final AtomicReference<State> stateRef = new AtomicReference<>(NOT_STARTED);
  private final AtomicReference<NakshaApp> nakshaRef = new AtomicReference<>(null);

  protected final Supplier<NakshaApp> nakshaAppInitializer;

  protected TestContext(Supplier<NakshaApp> nakshaAppInitializer) {
    this.nakshaAppInitializer = nakshaAppInitializer;
  }

  public final void start() {
    if (stateRef.compareAndSet(NOT_STARTED, STARTED)) {
      log.info("Starting test context...");
      setupStorage();
      startNaksha();
    } else {
      log.info("Not starting test context, expected {} state but context is in {} state", NOT_STARTED, stateRef.get());
    }
  }

  public final void stop() {
    if (stateRef.compareAndSet(STARTED, STOPPING)) {
      log.info("Stopping test context...");
      teardownStorage();
      stopNaksha();
      stateRef.set(STOPPED);
    } else {
      log.info("Not stopping test context, expected {} state but context is in {} state", NOT_STARTED, stateRef.get());
    }
  }

  public boolean isNotStarted() {
    return stateRef.get() == NOT_STARTED;
  }

  void setupStorage() {
    // empty by default, not required to be implemented
  }

  void teardownStorage() {
    // empty by default, not required to be implemented
  }

  private void startNaksha() {
    NakshaApp nakshaApp = nakshaAppInitializer.get();
    nakshaApp.start();
    nakshaRef.set(nakshaApp);
    try {
      Thread.sleep(5000); // wait for server to come up
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void stopNaksha() {
    nakshaRef.get().stopInstance();
  }

  enum State {
    NOT_STARTED,
    STARTED,
    STOPPING,
    STOPPED
  }
}
