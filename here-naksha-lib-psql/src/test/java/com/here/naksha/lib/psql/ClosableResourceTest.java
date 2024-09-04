package com.here.naksha.lib.psql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.util.ClosableRootResource;
import com.here.naksha.lib.core.util.CloseableResource;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

public class ClosableResourceTest {

  @Test
  void testLeakDetectionForRootResource() throws InterruptedException {
    // given
    AtomicBoolean destroyed = new AtomicBoolean(false);
    AtomicBoolean garbageCollected = new AtomicBoolean(false);
    PublicParentTestResource publicRootResourceFacade = new PublicParentTestResource(
        res -> destroyed.set(true),
        res -> garbageCollected.set(true)
    );

    // when facade hasn't been closed, but it was garbage collected.
    publicRootResourceFacade = null;
    System.gc();
    Thread.sleep(600); // wait 600ms as leak detector runs every 500ms

    // then
    assertTrue(garbageCollected.get());
    assertTrue(destroyed.get());
  }

  @Test
  void testLeakDetectionForChildren() throws Exception {
    // given
    AtomicBoolean destroyedParent = new AtomicBoolean(false);
    AtomicBoolean garbageCollectedParent = new AtomicBoolean(false);
    PublicParentTestResource publicRootResourceFacade = new PublicParentTestResource(
        res -> destroyedParent.set(true),
        res -> garbageCollectedParent.set(true)
    );

    AtomicBoolean destroyedChild = new AtomicBoolean(false);
    AtomicBoolean garbageCollectedChild = new AtomicBoolean(false);
    PublicTestResource publicChildResourceFacade = new PublicTestResource(
        publicRootResourceFacade,
        res -> destroyedChild.set(true),
        res -> garbageCollectedChild.set(true)
    );

    // when
    publicChildResourceFacade.close(); // commenting this line will show a leak - gc will never close children until parent is closed
    publicChildResourceFacade = null;
    System.gc();
    Thread.sleep(600); // wait 600ms as leak detector runs every 500ms

    // then
    assertFalse(destroyedParent.get());
    assertFalse(garbageCollectedParent.get());
    assertTrue(garbageCollectedChild.get());
    assertTrue(destroyedChild.get());
  }

  class TestResource extends CloseableResource<TestParentResource> {

    private final Consumer<TestResource> onDestruct;

    protected TestResource(@NotNull Object proxy, @Nullable TestParentResource parent, Consumer<TestResource> onDestruct) {
      super(proxy, parent);
      this.onDestruct = onDestruct;
    }

    @Override
    protected void destruct() {
      onDestruct.accept(this);
    }
  }

  class PublicTestResource implements AutoCloseable {

    private final @NotNull ClosableResourceTest.TestResource testResource;
    private final Consumer<PublicTestResource> onGarbageCollect;

    PublicTestResource(PublicParentTestResource parent, Consumer<TestResource> onDestruct, Consumer<PublicTestResource> onGarbageCollect) {
      this.testResource = new TestResource(this, parent.testResource, onDestruct);
      this.onGarbageCollect = onGarbageCollect;
    }


    @Override
    public void close() throws Exception {
      testResource.close();
    }

    @Override
    protected void finalize() throws Throwable {
      onGarbageCollect.accept(this);
    }
  }

  class TestParentResource extends ClosableRootResource {

    private final Consumer<TestParentResource> onDestruct;

    protected TestParentResource(@NotNull Object proxy, Consumer<TestParentResource> onDestruct) {
      super(proxy);
      this.onDestruct = onDestruct;
    }

    @Override
    protected void destruct() {
      onDestruct.accept(this);
    }
  }

  class PublicParentTestResource implements AutoCloseable {

    private final @NotNull ClosableResourceTest.TestParentResource testResource;
    private final Consumer<PublicParentTestResource> onGarbageCollect;

    PublicParentTestResource(Consumer<TestParentResource> onDestruct, Consumer<PublicParentTestResource> onGarbageCollect) {
      this.testResource = new TestParentResource(this, onDestruct);
      this.onGarbageCollect = onGarbageCollect;
    }

    @Override
    public void close() throws Exception {
      testResource.close();
    }

    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      onGarbageCollect.accept(this);
    }
  }

}
