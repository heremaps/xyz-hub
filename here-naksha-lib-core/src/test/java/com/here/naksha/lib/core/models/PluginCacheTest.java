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
package com.here.naksha.lib.core.models;

import static com.here.naksha.lib.core.models.PluginCache.eventHandlerConstructors;
import static com.here.naksha.lib.core.models.PluginCache.extensionCache;
import static com.here.naksha.lib.core.models.PluginCache.storageConstructors;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.lambdas.Fe3;
import com.here.naksha.lib.core.models.PluginCache.EventHandlerConstructorByConfig;
import com.here.naksha.lib.core.models.PluginCache.EventHandlerConstructorByTarget;
import com.here.naksha.lib.core.models.PluginCache.ExtensionConstructorByClassNameMap;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PluginCacheTest {

  public static class TestConfig {}

  public static class TestTarget {}

  public static class TestHandler implements IEventHandler {

    @Override
    public @NotNull Result processEvent(@NotNull IEvent event) {
      return new SuccessResult();
    }
  }

  public static class TestHandler2 extends TestHandler {
    public TestHandler2(@NotNull TestConfig config) {
      assertNotNull(config);
    }
  }

  public static class TestHandler3 extends TestHandler {
    public TestHandler3(@NotNull TestConfig config, @NotNull TestTarget target) {
      assertNotNull(config);
      assertNotNull(target);
    }
  }

  @BeforeAll
  static void beforeAll() {
    storageConstructors.clear();
    eventHandlerConstructors.clear();
    extensionCache.clear();
  }

  @Test
  void test1() throws Exception {
    Fe3<IEventHandler, INaksha, TestConfig, TestTarget> c =
        PluginCache.getEventHandlerConstructor(TestHandler.class.getName(), TestConfig.class, TestTarget.class);
    assertNotNull(c);
    IEventHandler handler = c.call(null, null, null);
    assertNotNull(handler);
    assertInstanceOf(SuccessResult.class, handler.processEvent(null));

    EventHandlerConstructorByConfig byConfig = eventHandlerConstructors.get(TestHandler.class.getName());
    assertNotNull(byConfig);
    EventHandlerConstructorByTarget byTarget = byConfig.get(TestConfig.class);
    assertNotNull(byTarget);
    Fe3<IEventHandler, INaksha, ?, ?> c2 = byTarget.get(TestTarget.class);
    assertSame(c, c2);
  }
  @Test
  void testExtensionCache() throws Exception {
    String extensionId="ExtensionId";
    ClassLoader classLoader=this.getClass().getClassLoader();
    Fe3<IEventHandler, INaksha, TestConfig, TestTarget> c =
        PluginCache.getEventHandlerConstructor(TestHandler.class.getName(), TestConfig.class, TestTarget.class,extensionId,classLoader);
    assertNotNull(c);
    IEventHandler handler = c.call(null, null, null);
    assertNotNull(handler);
    assertInstanceOf(SuccessResult.class, handler.processEvent(null));

    ExtensionConstructorByClassNameMap byName = extensionCache.get(extensionId);
    assertNotNull(byName);
    EventHandlerConstructorByTarget byTarget = byName.get(TestHandler.class.getName());
    assertNotNull(byTarget);
    Fe3<IEventHandler, INaksha, ?, ?> c2 = byTarget.get(TestTarget.class);
    assertSame(c, c2);

    PluginCache.removeExtensionCache(extensionId);
    assertNull(extensionCache.get(extensionId));

  }
}
