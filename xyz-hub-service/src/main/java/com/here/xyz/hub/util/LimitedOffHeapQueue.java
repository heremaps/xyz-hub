package com.here.xyz.hub.util;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.cache.OHCacheClient;
import com.here.xyz.hub.util.LimitedOffHeapQueue.OffHeapBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.caffinitas.ohc.OHCache;

/**
 * An extended version of the {@link LimitedQueue} which stores binary data held by its elements in an off-heap storage during the time the
 * elements are residing in this queue.
 *
 * When an element gets added to this queue its payload will taken from the heap and gets moved off-heap.
 * Once removing / fetching back the element from this queue it will be restored so that the payload is accessible using
 * {@link OffHeapBuffer#getPayload()}.
 * During the time the element resides in this queue its payload is not accessible. The method {@link OffHeapBuffer#getPayload()} will
 * throw an {@link IllegalStateException} when trying to access it.
 *
 * @param <E> The element type.
 */
public class LimitedOffHeapQueue<E extends OffHeapBuffer> extends LimitedQueue<E> {

  private static final long OH_TTL = 32_000; //ms
  private static final ScheduledExecutorService executors = Executors.newScheduledThreadPool(2);
  private static final OHCache<byte[], byte[]> ohStorage = OHCacheClient.createCache(
      (int) (Service.configuration.GLOBAL_MAX_QUEUE_SIZE * 1.1), executors, true);

  public LimitedOffHeapQueue(long maxSize, long maxByteSize) {
    super(maxSize, maxByteSize);
  }

  @Override
  public List<E> add(OffHeapBuffer element) {
    moveOffHeap(element);
    return super.add((E) element);
  }

  @Override
  public E remove() {
    E removed = super.remove();
    if (removed != null)
      ((OffHeapBuffer) removed).unStash();
    return removed;
  }

  private void moveOffHeap(OffHeapBuffer element) {
    byte[] stashedPayload = element.stash();
    ohStorage.put(element.ohKey, stashedPayload, OH_TTL);
  }

  private static void discardOHElement(OffHeapBuffer element) {
    if (element.payload.get() == null)
      ohStorage.remove(element.ohKey);
  }

  public static class PayloadVanishedException extends RuntimeException {
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
      byte[] tmpPayload = this.payload.getAndSet(null);
      if (tmpPayload == null) throw new IllegalStateException("Payload was already stashed.");
      ohKey = UUID.randomUUID().toString().getBytes();
      return tmpPayload;
    }

    private void unStash() throws PayloadVanishedException {
      byte[] tmpPayload = payload.get();
      if (tmpPayload == null) {
        tmpPayload = ohStorage.get(ohKey);
        discardOHElement(this);
      }

      if (tmpPayload == null)
        throw new PayloadVanishedException(this);

      payload.set(tmpPayload);
    }

    public final byte[] getPayload() throws IllegalStateException {
      byte[] payload = this.payload.get();
      if (payload == null)
        //Payload is still stashed and can't be accessed right now
        throw new IllegalStateException("Payload is still stashed off-heap and can not be accessed.");

      return payload;
    }
  }
}
