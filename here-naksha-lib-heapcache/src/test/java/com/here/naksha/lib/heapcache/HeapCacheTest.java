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
package com.here.naksha.lib.heapcache;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IMasterTransaction;
import com.here.naksha.lib.core.storage.ModifyFeaturesReq;
import java.lang.ref.WeakReference;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class HeapCacheTest {

  static void gc(@NotNull WeakReference<?> ref) {
    System.gc();
    while (ref.get() != null) {
      Thread.yield();
      System.gc();
    }
  }

  @Test
  void CacheSoftReferenceTest() {
    final HeapCache cache = new HeapCache(new HeapCacheConfig(null));
    try (final IMasterTransaction tx = cache.openMasterTransaction(cache.createSettings())) {
      tx.writeFeatures(XyzFeature.class, new CollectionInfo("foo"))
          .modifyFeatures(new ModifyFeaturesReq<>().insert(new XyzFeature("x")));
      tx.commit();

      XyzFeature feature =
          tx.readFeatures(XyzFeature.class, new CollectionInfo("foo")).getFeatureById("x");
      assertNotNull(feature);
      assertEquals("x", feature.getId());
    }
  }

  @Test
  void CacheWeakReferenceTest() {
    final HeapCache cache = new HeapCache(new HeapCacheConfig(null));
    try (final IMasterTransaction tx = cache.openMasterTransaction(cache.createSettings())) {
      tx.writeFeatures(XyzFeature.class, new CollectionInfo("foo"))
          .modifyFeatures(new ModifyFeaturesReq<>().insert(new XyzFeature("x")));
      tx.commit();

      gc(new WeakReference<>(new Object()));

      XyzFeature feature =
          tx.readFeatures(XyzFeature.class, new CollectionInfo("foo")).getFeatureById("x");
      assertNull(feature);
    }
  }

  @Test
  void cacheGetFeaturesByIdTest() {
    final HeapCache cache = new HeapCache(new HeapCacheConfig(null));
    try (final IMasterTransaction tx = cache.openMasterTransaction(cache.createSettings())) {
      tx.writeFeatures(XyzFeature.class, new CollectionInfo("bar"))
          .modifyFeatures(new ModifyFeaturesReq<>().insert(new XyzFeature("r")));
      tx.commit();

      XyzFeature feature = tx.readFeatures(XyzFeature.class, new CollectionInfo("bar"))
          .getFeaturesById("r")
          .getFeature();
      assertNotNull(feature);
      assertEquals("r", feature.getId());
    }
  }

  @Mock
  private CacheChangeListener listener;

  @Test
  public void testCacheListener() {
    MockitoAnnotations.initMocks(this);

    HeapCache cache = new HeapCache(new HeapCacheConfig(null));
    cache.addListener(listener);

    try (final IMasterTransaction tx = cache.openMasterTransaction(cache.createSettings())) {
      tx.writeFeatures(XyzFeature.class, new CollectionInfo("foo"))
          .modifyFeatures(new ModifyFeaturesReq<>().insert(new XyzFeature("x")));
      tx.commit();

      XyzFeature feature =
          tx.readFeatures(XyzFeature.class, new CollectionInfo("foo")).getFeatureById("x");

      String key = "foo";

      // Simulate adding a cache entry
      cache.putCacheEntry(key, feature);

      // Verify that the listener was notified
      verify(listener, times(1)).onCacheEntryAdded(key, feature);

      // Simulate updating a cache entry
      cache.updateCacheEntry(key, feature);

      // Verify that the listener was notified
      verify(listener, times(1)).onCacheEntryUpdated(key, feature);

      // Simulate removing a cache entry
      cache.removeCacheEntry(key);

      // Verify that the listener was notified
      verify(listener, times(1)).onCacheEntryRemoved(key);

      // Cleanup and remove the listener
      cache.removeListener(listener);
    }
  }
}
