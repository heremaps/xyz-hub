package com.here.naksha.lib.extmanager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.ExtensionConfig;
import com.here.naksha.lib.extmanager.helpers.AmazonS3Helper;
import com.here.naksha.lib.extmanager.helpers.ClassLoaderHelper;
import com.here.naksha.lib.extmanager.helpers.FileHelper;
import java.io.File;
import java.io.IOException;
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

    fileClient =extensionCache.getJarClient("file://bucket/test.jar");
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

      ClassLoader loader=extensionCache.getClassLoaderById(extensionConfig.getExtensions().get(0).getId());
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
}
