package com.here.naksha.storage.http.cache;

import com.here.naksha.storage.http.RequestSender;
import com.here.naksha.storage.http.RequestSender.KeyProperties;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RequestSenderCacheTest {

  public static final String EXAMPLE_URL = "www.example.naksha.com";

  public static final int EXAMPLE_CONNECTION_TIMEOUT = 1;

  public static final int EXAMPLE_SOCKET_TIMEOUT = 1;

  public static final Map<String, String> EXAMPLE_HEADERS = Map.of("Authorization", "Bearer exampleToken", "Content-Type", "application/json");
  public static final Map<String, String> MODIFIED_HEADERS = Map.of("Authorization", "Bearer modifiedToken", "Content-Type", "application/json");

  public static final String ID_1 = "id_1";
  public static final String ID_2 = "id_2";
  public static final String ID_3 = "id_3";
  public static final KeyProperties PROP_ID_1 = new KeyProperties(
          ID_1,
          EXAMPLE_URL,
          EXAMPLE_HEADERS,
          EXAMPLE_CONNECTION_TIMEOUT,
          EXAMPLE_SOCKET_TIMEOUT
  );

  public static final KeyProperties PROP_ID_1_COPY = new KeyProperties(
          ID_1,
          EXAMPLE_URL,
          EXAMPLE_HEADERS,
          EXAMPLE_CONNECTION_TIMEOUT,
          EXAMPLE_SOCKET_TIMEOUT
  );

  public static final KeyProperties PROP_ID_1_INT_CHANGED = new KeyProperties(
          ID_1,
          EXAMPLE_URL,
          EXAMPLE_HEADERS,
          EXAMPLE_CONNECTION_TIMEOUT,
          2
  );

  public static final KeyProperties PROP_ID_1_MAP_COPIED = new KeyProperties(
          ID_1,
          EXAMPLE_URL,
          Map.copyOf(EXAMPLE_HEADERS),
          EXAMPLE_CONNECTION_TIMEOUT,
          EXAMPLE_SOCKET_TIMEOUT
  );

  public static final KeyProperties PROP_ID_1_MAP_CHANGED = new KeyProperties(
          ID_1,
          EXAMPLE_URL,
          MODIFIED_HEADERS,
          EXAMPLE_CONNECTION_TIMEOUT,
          EXAMPLE_SOCKET_TIMEOUT
  );

  public static final KeyProperties PROP_ID_2 = new KeyProperties(
          ID_2,
          EXAMPLE_URL,
          EXAMPLE_HEADERS,
          EXAMPLE_CONNECTION_TIMEOUT,
          EXAMPLE_SOCKET_TIMEOUT
  );

  public static final KeyProperties PROP_ID_3 = new KeyProperties(
          ID_3,
          EXAMPLE_URL,
          EXAMPLE_HEADERS,
          EXAMPLE_CONNECTION_TIMEOUT,
          EXAMPLE_SOCKET_TIMEOUT
  );


  @Test
  void testOneId() {
    // Setup
    ConcurrentMap<String, RequestSender> senders = new ConcurrentHashMap<>();
    RequestSenderCache cache = new RequestSenderCache(
            senders,
            8,
            TimeUnit.HOURS
    );

    assertEquals(PROP_ID_1, PROP_ID_1_COPY);
    assertNotSame(PROP_ID_1, PROP_ID_1_COPY);

    // Tests
    assertEquals(0, senders.size());

    RequestSender senderId1 = cache.getSenderWith(PROP_ID_1);
    assertEquals(1, senders.size());

    RequestSender senderId1Copy = cache.getSenderWith(PROP_ID_1_COPY);
    assertEquals(1, senders.size());
    assertSame(senderId1, senderId1Copy);

    RequestSender senderId1IntChanged
            = cache.getSenderWith(PROP_ID_1_INT_CHANGED);
    assertEquals(1, senders.size());
    assertNotEquals(senderId1Copy, senderId1IntChanged);
  }

  @Test
  void testOneIdMapChange() {
    // Setup
    ConcurrentMap<String, RequestSender> senders = new ConcurrentHashMap<>();
    RequestSenderCache cache = new RequestSenderCache(
            senders,
            8,
            TimeUnit.HOURS
    );

    assertEquals(PROP_ID_1, PROP_ID_1_MAP_COPIED);
    assertNotSame(PROP_ID_1, PROP_ID_1_MAP_COPIED);

    // Tests
    assertEquals(0, senders.size());

    RequestSender senderId1 = cache.getSenderWith(PROP_ID_1);
    assertEquals(1, senders.size());

    RequestSender senderId1MapCopied = cache.getSenderWith(PROP_ID_1_MAP_COPIED);
    assertEquals(1, senders.size());
    assertSame(senderId1, senderId1MapCopied);

    RequestSender senderId1MapChanged
            = cache.getSenderWith(PROP_ID_1_MAP_CHANGED);
    assertEquals(1, senders.size());
    assertNotEquals(senderId1MapCopied, senderId1MapChanged);
  }

  @Test
  void testMoreIds() {
    // Setup
    ConcurrentMap<String, RequestSender> senders = new ConcurrentHashMap<>();
    RequestSenderCache cache = new RequestSenderCache(
            senders,
            8,
            TimeUnit.HOURS
    );

    // Tests
    assertEquals(0, senders.size());

    RequestSender senderId1 = cache.getSenderWith(PROP_ID_1);
    assertEquals(1, senders.size());

    RequestSender senderId2 = cache.getSenderWith(PROP_ID_2);
    assertEquals(2, senders.size());
    assertNotEquals(senderId1, senderId2);

    RequestSender senderId3 = cache.getSenderWith(PROP_ID_3);
    assertEquals(3, senders.size());
    assertNotEquals(senderId1, senderId3);

    RequestSender newSenderId1 = cache.getSenderWith(PROP_ID_1);
    assertEquals(3, senders.size());
    assertSame(senderId1, newSenderId1);
  }

  @Test
  void testCleanup() throws InterruptedException {
    // Setup
    int cleanPeriodMs = 1000;
    ConcurrentMap<String, RequestSender> senders = new ConcurrentHashMap<>();
    RequestSenderCache cache = new RequestSenderCache(
            senders,
            cleanPeriodMs,
            TimeUnit.MILLISECONDS
    );

    // Tests
    assertEquals(0, senders.size());
    cache.getSenderWith(PROP_ID_1);
    cache.getSenderWith(PROP_ID_2);
    cache.getSenderWith(PROP_ID_3);
    assertEquals(3, senders.size());
    Thread.sleep(cleanPeriodMs + 100);
    assertEquals(0, senders.size());
    cache.getSenderWith(PROP_ID_1);
    assertEquals(1, senders.size());
  }


  @RepeatedTest(10)
  void testCleanupConcurrency() throws InterruptedException {
    // Setup
    int cleanPeriodMs = 1;
    ConcurrentMap<String, RequestSender> senders = new ConcurrentHashMap<>();
    RequestSenderCache cache = new RequestSenderCache(
            senders,
            cleanPeriodMs,
            TimeUnit.MILLISECONDS
    );

    // Tests
    assertEquals(0, senders.size());

    assertTrue(cache.getSenderWith(PROP_ID_1).hasKeyProps(PROP_ID_1));
    assertTrue(cache.getSenderWith(PROP_ID_2).hasKeyProps(PROP_ID_2));
    assertTrue(cache.getSenderWith(PROP_ID_3).hasKeyProps(PROP_ID_3));
    Thread.sleep(cleanPeriodMs + 100);
    assertEquals(0, senders.size());
  }

  @RepeatedTest(10)
  void testGetSenderConcurrency() throws InterruptedException {
    // Setup
    ConcurrentMap<String, RequestSender> senders = new ConcurrentHashMap<>();
    RequestSenderCache cache = new RequestSenderCache(
            senders,
            8,
            TimeUnit.HOURS
    );
    AtomicReference<RequestSender> senderId1 = new AtomicReference<>();
    AtomicReference<RequestSender> senderId1Copy = new AtomicReference<>();
    AtomicReference<RequestSender> senderId1IntChanged = new AtomicReference<>();
    AtomicReference<RequestSender> senderId1MapChanged = new AtomicReference<>();

    // Tests
    List<Thread> threads = List.of(
            new Thread(() -> senderId1.set(cache.getSenderWith(PROP_ID_1))),
            new Thread(() -> senderId1Copy.set(cache.getSenderWith(PROP_ID_1_COPY))),
            new Thread(() -> senderId1IntChanged.set(cache.getSenderWith(PROP_ID_1_INT_CHANGED))),
            new Thread(() -> senderId1MapChanged.set(cache.getSenderWith(PROP_ID_1_MAP_CHANGED)))
    );
    threads.forEach(Thread::start);
    for (Thread thread : threads) {
      thread.join();
    }

    assertTrue(senderId1.get().hasKeyProps(PROP_ID_1));
    assertTrue(senderId1Copy.get().hasKeyProps(PROP_ID_1_COPY));
    assertTrue(senderId1IntChanged.get().hasKeyProps(PROP_ID_1_INT_CHANGED));
    assertTrue(senderId1MapChanged.get().hasKeyProps(PROP_ID_1_MAP_CHANGED));
  }

}