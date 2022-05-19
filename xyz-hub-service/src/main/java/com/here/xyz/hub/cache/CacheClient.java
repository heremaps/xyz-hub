/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.cache;

import io.vertx.core.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

public interface CacheClient {

	Future<byte[]> get(String key);

	/**
	 *
	 * @param key
	 * @param value
	 * @param ttl The live time of the cache-record in seconds
	 */
	void set(String key, byte[] value, long ttl);

	void remove(String key);

	@Nonnull
	static CacheClient getInstance() {
		return new MultiLevelCacheClient(OHCacheClient.getInstance(), RedisCacheClient.getInstance());
	}

	/**
	 * Acquire a distributed lock for the given, which should automatically be released after the
	 * given TTL.
	 *
	 * @param key the key
	 * @param ttl the time-to-live.
	 * @param ttlUnit the unit in which the TTL was provided.
	 * @return true if the lock was acquired; false otherwise.
	 */
	default boolean acquireLock(@Nonnull String key, long ttl, @Nonnull TimeUnit ttlUnit) {
		return false;
	}

	/**
	 * Releases the lock acquired by {@link #acquireLock(String, long, TimeUnit)}. The key must match
	 * with the lock acquired previously.
	 * @param key the key which the lock was acquired
	 */
	default void releaseLock(@Nonnull String key) {}

	void shutdown();

}
