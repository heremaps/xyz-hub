package com.here.naksha.lib.extmanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.features.Extension;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

public class ExtensionManagerTest extends BaseSetup {
  INaksha naksha=mock(INaksha.class);
  @BeforeEach
  public void init(){
    when(naksha.getExtensionConfig()).thenReturn(getExtensionConfig());
  }

  @Test
  public void testGetClassLoaderByIdAndGetCachedExtensions()  {
    List<Extension> extList=new ArrayList<>();
    extList.add(new Extension("child_extension_1","url","1.0",null));

    ClassLoader loader=mock(ClassLoader.class);
    try(MockedConstruction<ExtensionCache> mockExtensionCache=mockConstruction(ExtensionCache.class,(mock,context)->{
      when(mock.getClassLoaderById("AnyString")).thenReturn(loader);
      when(mock.getCachedExtensions()).thenReturn(extList);
    })) {
      ExtensionManager extensionManager = spy(ExtensionManager.getInstance(naksha));

      ClassLoader clsLoader = extensionManager.getClassLoader("AnyString");
      assertEquals(loader, clsLoader);

      clsLoader = extensionManager.getClassLoader("Nothing");
      assertNull(clsLoader);

      List<Extension> extensions = extensionManager.getCachedExtensions();
      Assertions.assertEquals(extList.size(),extensions.size());
    }
  }
}
