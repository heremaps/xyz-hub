package com.here.naksha.lib.extmanager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.ExtensionConfig;
import com.here.naksha.lib.core.models.PluginCache;
import com.here.naksha.lib.core.models.features.Extension;
import com.here.naksha.lib.extmanager.helpers.AmazonS3Helper;
import com.here.naksha.lib.extmanager.helpers.ClassLoaderHelper;
import com.here.naksha.lib.extmanager.helpers.FileHelper;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;

public class ExtensionCacheTest extends BaseSetup {

  @Mock
  INaksha naksha;
  @Test
  public void testBuildExtensionCache() throws IOException {
    ClassLoader classLoader=mock(ClassLoader.class);
    ExtensionConfig extensionConfig=getExtensionConfig();
    AmazonS3Helper s3Helper=mock(AmazonS3Helper.class);
    when(s3Helper.getFile(anyString())).thenReturn(new File(""));

    try(MockedStatic<ClassLoaderHelper> mockedStatic=mockStatic(ClassLoaderHelper.class)) {
      when(ClassLoaderHelper.getClassLoader(any(),anyList())).thenReturn(classLoader);
      ExtensionCache extensionCache =spy( new ExtensionCache(naksha));
      doReturn(s3Helper).when(extensionCache).getJarClient(anyString());

      extensionCache.buildExtensionCache(extensionConfig);
      Assertions.assertEquals(2,extensionCache.getCacheLength());

      extensionConfig.getExtensions().remove(0);
      extensionCache.buildExtensionCache(extensionConfig);
      Assertions.assertEquals(1,extensionCache.getCacheLength());
    }
  }

  @Test
  public void testGetJarClient(){
    ExtensionCache extensionCache=new ExtensionCache(naksha);
    FileClient fileClient =extensionCache.getJarClient("s3://bucket/test.jar");
    Assertions.assertTrue(fileClient instanceof AmazonS3Helper);

    FileClient fileClient1 =extensionCache.getJarClient("s3://bucket/test1.jar");
    Assertions.assertEquals(fileClient, fileClient1);

    fileClient =extensionCache.getJarClient("file:///bucket/test.jar");
    Assertions.assertTrue(fileClient instanceof FileHelper);

    Assertions.assertThrows(UnsupportedOperationException.class,()->extensionCache.getJarClient("error://bucket/test.jar"));
  }

  @Test
  public void testGetClassLoaderById() throws IOException {
    ClassLoader classLoader=mock(ClassLoader.class);
    ExtensionConfig extensionConfig=getExtensionConfig();
    AmazonS3Helper s3Helper=mock(AmazonS3Helper.class);
    when(s3Helper.getFile(anyString())).thenReturn(new File(""));

    try(MockedStatic<ClassLoaderHelper> mockedStatic=mockStatic(ClassLoaderHelper.class)) {
      when(ClassLoaderHelper.getClassLoader(any(),anyList())).thenReturn(classLoader);
      ExtensionCache extensionCache =spy(new ExtensionCache(naksha));
      doReturn(s3Helper).when(extensionCache).getJarClient(anyString());

      extensionCache.buildExtensionCache(extensionConfig);
      Assertions.assertEquals(2,extensionCache.getCacheLength());

      ClassLoader loader=extensionCache.getClassLoaderById(extensionConfig.getExtensions().get(0).getEnv()+":"+extensionConfig.getExtensions().get(0).getId());
      Assertions.assertNotNull(loader);
      Assertions.assertEquals(classLoader,loader);
    }
  }

  @Test
  public void testGetCachedExtensions() throws IOException {
    ClassLoader classLoader=mock(ClassLoader.class);
    ExtensionConfig extensionConfig=getExtensionConfig();
    AmazonS3Helper s3Helper=mock(AmazonS3Helper.class);
    when(s3Helper.getFile(anyString())).thenReturn(new File(""));

    try(MockedStatic<ClassLoaderHelper> mockedStatic=mockStatic(ClassLoaderHelper.class)) {
      when(ClassLoaderHelper.getClassLoader(any(),anyList())).thenReturn(classLoader);
      ExtensionCache extensionCache =spy( new ExtensionCache(naksha));
      doReturn(s3Helper).when(extensionCache).getJarClient(anyString());

      extensionCache.buildExtensionCache(extensionConfig);
      Assertions.assertEquals(2,extensionCache.getCachedExtensions().size());


      extensionConfig.getExtensions().remove(0);
      extensionCache.buildExtensionCache(extensionConfig);
      Assertions.assertEquals(1,extensionCache.getCachedExtensions().size());
      Assertions.assertEquals(extensionConfig.getExtensions().get(0).getId(),extensionCache.getCachedExtensions().get(0).getId());
    }
  }

  @Test
  public void testGetCachedExtensionsVersionUpdate() throws IOException {
    ClassLoader classLoaderOld = mock(ClassLoader.class);
    ClassLoader classLoaderNew = mock(ClassLoader.class);

    ExtensionConfig extensionConfigOld = getExtensionConfig();
    ExtensionConfig extensionConfigNew = getExtensionConfig("src/test/resources/data/extensionNewVersion.txt");

    AmazonS3Helper s3Helper=mock(AmazonS3Helper.class);
    when(s3Helper.getFile(anyString())).thenReturn(new File(""));

    try(MockedStatic<ClassLoaderHelper> mockedStatic=mockStatic(ClassLoaderHelper.class);
        MockedStatic<PluginCache> pluginCacheMock = mockStatic(PluginCache.class)
    ) {
      ExtensionCache extensionCache =spy( new ExtensionCache(naksha));
      doReturn(s3Helper).when(extensionCache).getJarClient(anyString());

      mockedStatic.when(() -> ClassLoaderHelper.getClassLoader(any(), anyList()))
              .thenReturn(classLoaderOld);
      extensionCache.buildExtensionCache(extensionConfigOld);
      List<Extension> oldCached = extensionCache.getCachedExtensions();
      Assertions.assertEquals(2, oldCached.size());

      String ext1KeyOld = oldCached.get(0).getEnv() + ":" + oldCached.get(0).getId();
      String ext2Key = oldCached.get(1).getEnv() + ":" + oldCached.get(1).getId();

      Assertions.assertEquals(classLoaderOld, extensionCache.getClassLoaderById(ext1KeyOld));
      Assertions.assertEquals(classLoaderOld, extensionCache.getClassLoaderById(ext2Key));

      mockedStatic.when(() -> ClassLoaderHelper.getClassLoader(any(), anyList()))
              .thenReturn(classLoaderNew);
      extensionCache.buildExtensionCache(extensionConfigNew);
      List<Extension> newCached = extensionCache.getCachedExtensions();
      Assertions.assertEquals(1, newCached.size());

      Extension updatedExt = newCached.get(0);
      Assertions.assertEquals("child_extension_1", updatedExt.get("extensionId"));
      Assertions.assertEquals("2.0", updatedExt.getVersion());

      String ext1KeyNew = updatedExt.getEnv() + ":" + updatedExt.getId();

      // Verify that the classloader has been updated from old to new
      Assertions.assertEquals(classLoaderNew, extensionCache.getClassLoaderById(ext1KeyNew));
      Assertions.assertNotEquals(classLoaderOld, extensionCache.getClassLoaderById(ext1KeyNew));

      // Verify that child_extension_2 has been removed
      Assertions.assertNull(extensionCache.getClassLoaderById(ext2Key));

      // Verify that removeExtensionCache called 2 times
      pluginCacheMock.verify(() -> PluginCache.removeExtensionCache(ext2Key), times(2));

    }
  }
}
