package com.here.xyz.hub.util;

import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.cache.OHCacheClient;
import com.here.xyz.hub.util.LimitedOffHeapQueue.OffHeapBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.caffinitas.ohc.OHCache;

/**
 * An extended version of the {@link LimitedQueue} which stores binary data held by its elements in an off-heap storage during the time the
 * elements are residing in this queue.
 *
 * When an element gets added to this queue its payload will be taken from the heap and gets moved off-heap.
 * Once removing / fetching back the element from this queue it will be restored using {@link OffHeapBuffer#getPayload()}
 * or {@link OffHeapBuffer#consumePayload()}.
 *
 * @param <E> The element type.
 */
public class LimitedOffHeapQueue<E extends OffHeapBuffer> extends LimitedQueue<E> {

  private static final long OH_TTL = 32_000; //ms
  private static final ScheduledExecutorService executors = new ScheduledThreadPoolExecutor(2, Core.newThreadFactory("oh-queues"));
  private static final OHCache<byte[], byte[]> ohStorage = OHCacheClient.createCache(
      (int) (Service.configuration.GLOBAL_MAX_QUEUE_SIZE * 1.1), executors, true);

  public LimitedOffHeapQueue(long maxSize, long maxByteSize) {
    super(maxSize, maxByteSize);
  }

  @Override
  public List<E> add(OffHeapBuffer element) {
    moveOffHeap(element);
    List<E> discarded = super.add((E) element);
    //Also discard the according off-heap elements explicitly. Otherwise the OHC would pick the elements to be discarded.
    discarded.forEach(e -> discardOHElement(e));
    return discarded;
  }

  private void moveOffHeap(OffHeapBuffer element) {
    element.stash();
  }

  private static void discardOHElement(OffHeapBuffer element) {
    if (element.payload.get() == null && element.ohKey != null)
      ohStorage.remove(element.ohKey);
  }

  public static class PayloadVanishedException extends Exception {
    public final String ohKey;

    private PayloadVanishedException(OffHeapBuffer item) {
      super("The item is not available in the off-heap queue anymore.");
      ohKey = new String(item.ohKey);
    }
  }

  public static class OffHeapBuffer implements ByteSizeAware {

    private AtomicReference<byte[]> payload = new AtomicReference<>();
    private byte[] ohKey;
    private long payloadSize;
    private AtomicBoolean consumed = new AtomicBoolean();

    public OffHeapBuffer(byte[] payload) {
      assert payload != null;
      this.payload.set(payload);
      payloadSize = payload.length;
    }

    @Override
    public final long getByteSize() {
      return payloadSize;
    }

    /**
     * Will be called by an OffHeapQueue which wants to "stash" this OffHeapBuffer-item in the off-heap storage.
     * The local reference to the payload will be dropped when stashing it.
     * @return The bytes to be stored in the off-heap storage.
     */
    private byte[] stash() throws IllegalStateException {
      byte[] tmpPayload = this.payload.get();
      if (tmpPayload == null) throw new IllegalStateException("Payload was already stashed.");
      byte[] key = UUID.randomUUID().toString().getBytes();
      ohStorage.put(key, tmpPayload, Service.currentTimeMillis() + OH_TTL);
      ohKey = key;
      this.payload.set(null);
      return tmpPayload;
    }

    private void unStash() throws PayloadVanishedException {
      byte[] tmpPayload = payload.get();
      if (tmpPayload == null) {
        if (ohKey == null)
          throw new IllegalStateException("Payload is not stashed. Can not un-stash it.");
        tmpPayload = ohStorage.get(ohKey);
        discardOHElement(this);
      }

      if (tmpPayload == null)
        throw new PayloadVanishedException(this);

      payload.set(tmpPayload);
    }

    public final byte[] getPayload() throws PayloadVanishedException {
      if (consumed.get())
        throw new IllegalStateException("Payload was already consumed.");
      return getPayloadInternal();
    }

    private byte[] getPayloadInternal() throws PayloadVanishedException {
      byte[] payload = this.payload.get();
      if (payload == null) {
        //Payload is stashed and can't be accessed right now. Un-stashing it.
        unStash();
        return this.payload.get();
      }

      return payload;
    }

    public final byte[] consumePayload() throws PayloadVanishedException {
      if (!consumed.compareAndSet(false, true))
        throw new IllegalStateException("Payload was already consumed.");
      byte[] payload = getPayloadInternal();
      this.payload.set(null);
      return payload;
    }
  }
}
