package com.here.naksha.lib.core.util;

import static com.here.naksha.lib.core.util.IoHelp.readConfigFromHomeOrResource;
import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.lib.core.util.IoHelp.LoadedConfig;
import java.io.IOException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

class IoHelpTest {

    // This app name should not exists.
    private static final String APP_NAME = RandomStringUtils.random(40);

    public static class ConfigTest {

        public int theInt;
        public boolean theBool;
        public String theString;
        public String env;
    }

    @Test
    void test_configFileFromResources() throws IOException {
        final LoadedConfig<ConfigTest> loadedConfig =
                readConfigFromHomeOrResource("iohelp_config_test.json", false, APP_NAME, ConfigTest.class);
        assertNotNull(loadedConfig);
        final ConfigTest config = loadedConfig.config();
        assertNotNull(config);
        assertEquals(100, config.theInt);
        assertTrue(config.theBool);
        assertEquals("Hello World!", config.theString);
        assertEquals(config.env, "default");
    }
}
