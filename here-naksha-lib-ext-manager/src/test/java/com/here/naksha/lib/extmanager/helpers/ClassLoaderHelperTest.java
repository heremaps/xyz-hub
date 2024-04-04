package com.here.naksha.lib.extmanager.helpers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClassLoaderHelperTest {

  @Test
  public void testGetClassLoader() throws IOException {
    File file=File.createTempFile("tmpfile",".jar");
    ClassLoader classLoader= ClassLoaderHelper.getClassLoader(file,new ArrayList<>());
    Assertions.assertNotNull(classLoader);
  }

}
