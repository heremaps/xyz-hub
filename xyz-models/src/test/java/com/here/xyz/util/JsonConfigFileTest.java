package com.here.xyz.util;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import javax.annotation.Nullable;
import org.junit.Test;

@SuppressWarnings("DuplicatedCode")
public class JsonConfigFileTest {

  private static final String test_config_file = "/test_config_file.json";
  private static final String test_config_file2 = "/test_config_file2.json";

  @EnvName("TEST_CONFIG_PATH")
  private static class TestConfig extends JsonConfigFile<TestConfig> {

    TestConfig() {
      super(test_config_file);
    }

    TestConfig(String filename) {
      super(filename);
    }

    protected void info(String message) {
      System.out.println(message);
    }

    protected void error(String message, Throwable t) {
      System.err.println(message);
      if (t != null) {
        t.printStackTrace(System.err);
      }
    }

    @Nullable
    @Override
    protected String envPrefix() {
      return "TEST_";
    }

    public int theInt = 1;
    public boolean theBool = false;
    public String theString = "foo";
    public String env = "env-default";
  }

  public static class TestConfig2 extends TestConfig {

    TestConfig2() {
      super(test_config_file2);
    }
  }

  public static class TestConfigWithAnnotation extends TestConfig {

    @EnvName(value = "BAR", prefix = true)
    public String testAnnotation = "foo";
  }

  public static class TestConfigWithAnnotationWithoutPrefix extends TestConfig {

    @EnvName("BAR")
    public String testAnnotation = "foo";
  }

  @Test
  public void test_defaults() {
    final TestConfig testConfig = new TestConfig();
    assertEquals(1, testConfig.theInt);
    assertFalse(testConfig.theBool);
    assertEquals("foo", testConfig.theString);
    assertEquals("env-default", testConfig.env);
  }

  @Test
  public void test_withMap() {
    final TestConfig testConfig = new TestConfig();
    final LinkedHashMap<String, Object> testValues = new LinkedHashMap<>();
    testValues.put("theInt", 5);
    testValues.put("theBool", true);
    testValues.put("theString", "string");
    testValues.put("env", "env");
    testConfig.load(testValues, null);
    assertEquals(5, testConfig.theInt);
    assertTrue(testConfig.theBool);
    assertEquals("string", testConfig.theString);
    assertEquals("env", testConfig.env);
  }

  @Nullable
  private static String test_withMapAndEnv_getEnv(String key) {
    if ("TEST_ENV".equals(key)) {
      return "env-overridden";
    }
    if ("TEST_THESTRING".equals(key)) {
      return "null";
    }
    return null;
  }

  @Test
  public void test_withMapAndEnv() {
    final TestConfig testConfig = new TestConfig();
    final LinkedHashMap<String, Object> testValues = new LinkedHashMap<>();
    testValues.put("theInt", 5);
    testValues.put("theBool", true);
    testValues.put("theString", "string");
    testValues.put("env", "env");
    testConfig.load(testValues, JsonConfigFileTest::test_withMapAndEnv_getEnv);
    assertEquals(5, testConfig.theInt);
    assertTrue(testConfig.theBool);
    assertNull(testConfig.theString);
    assertEquals("env-overridden", testConfig.env);
  }

  @Nullable
  private static String test_config_file_getEnv(String key) {
    return null;
  }

  @Test
  public void test_config_file() throws Exception {
    final TestConfig testConfig = new TestConfig();
    testConfig.load(JsonConfigFileTest::test_config_file_getEnv);
    assertEquals(100, testConfig.theInt);
    assertTrue(testConfig.theBool);
    assertEquals("Hello World!", testConfig.theString);
    assertEquals("default", testConfig.env);
  }

  @Nullable
  private static String test_env_getEnv(String key) {
    if ("TEST_ENV".equals(key)) {
      return "noDefault";
    }
    if ("TEST_THEINT".equals(key)) {
      return "10";
    }
    if ("TEST_THEBOOL".equals(key)) {
      return "false";
    }
    if ("TEST_THESTRING".equals(key)) {
      return "boobar";
    }
    return null;
  }

  @Test
  public void test_env() throws Exception {
    final TestConfig testConfig = new TestConfig();
    testConfig.load(JsonConfigFileTest::test_env_getEnv);
    assertEquals(10, testConfig.theInt);
    assertFalse(testConfig.theBool);
    assertEquals("boobar", testConfig.theString);
    assertEquals("noDefault", testConfig.env);
  }

  @Nullable
  private static String test_config_file2_getEnv(String key) {
    if ("TEST_CONFIG_PATH".equals(key)) {
      return "/";
    }
    return null;
  }

  @Test
  public void test_config_file2() throws Exception {
    final TestConfig2 testConfig = new TestConfig2();
    testConfig.load(JsonConfigFileTest::test_config_file2_getEnv);
    assertEquals(-100, testConfig.theInt);
    assertTrue(testConfig.theBool);
    assertEquals("Hello World!", testConfig.theString);
    assertEquals("default", testConfig.env);
  }

  @Nullable
  private static String test_nullable_getEnv(String key) {
    if ("TEST_THESTRING".equals(key)) {
      return "null";
    }
    return null;
  }

  @Test
  public void test_nullable() throws Exception {
    final TestConfig testConfig = new TestConfig();
    testConfig.load(JsonConfigFileTest::test_nullable_getEnv);
    assertEquals(100, testConfig.theInt);
    assertTrue(testConfig.theBool);
    assertNull(testConfig.theString);
    assertEquals("default", testConfig.env);
  }

  @Nullable
  private static String test_annotationWithPrefix_getEnv(String key) {
    if ("TEST_BAR".equals(key)) {
      return "bar";
    }
    return null;
  }

  @Test
  public void test_annotationWithPrefix() throws Exception {
    final TestConfigWithAnnotation testConfig = new TestConfigWithAnnotation();
    testConfig.load(JsonConfigFileTest::test_annotationWithPrefix_getEnv);
    assertEquals("bar", testConfig.testAnnotation);
  }

  @Nullable
  private static String test_annotationWithoutPrefix_getEnv(String key) {
    if ("BAR".equals(key)) {
      return "bar";
    }
    return null;
  }

  @Test
  public void test_annotationWithoutPrefix() throws Exception {
    final TestConfigWithAnnotationWithoutPrefix testConfig = new TestConfigWithAnnotationWithoutPrefix();
    testConfig.load(JsonConfigFileTest::test_annotationWithoutPrefix_getEnv);
    assertEquals("bar", testConfig.testAnnotation);
  }

}