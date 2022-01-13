package com.here.xyz.hub.connectors;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.Service.Config;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Embedded;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConnectorConfigTest {

  static Connector c = new Connector();

  @Before
  public void before() {
    Service.configuration = new Config();
    Service.configuration.ENVIRONMENT_NAME = "euWest1";
    c.id = "test";

    RemoteFunctionConfig euWest1 = new Embedded();
    euWest1.id = "euWest1";
    c.remoteFunctions.put("euWest1", euWest1);

    RemoteFunctionConfig usEast1 = new Embedded();
    usEast1.id = "usEast1";
    usEast1.defaultConfig = true;
    c.remoteFunctions.put("usEast1", usEast1);

    RemoteFunctionConfig euCentral1 = new Embedded();
    euCentral1.id = "euCentral1";
    c.remoteFunctions.put("euCentral1", euCentral1);
  }

  @Test
  public void testDefaultRemoteFunctionConfig() {
    RemoteFunctionConfig defaultConfig = c.getDefaultRemoteFunctionConfig();
    Assert.assertNotNull(defaultConfig);
    Assert.assertEquals("usEast1", defaultConfig.id);

    c.remoteFunctions.remove("euWest1");
    defaultConfig = c.getDefaultRemoteFunctionConfig();
    Assert.assertNotNull(defaultConfig);

    c.remoteFunctions.remove("usEast1");
    defaultConfig = c.getDefaultRemoteFunctionConfig();
    Assert.assertNull(defaultConfig);
  }

  @Test
  public void testGetRemoteFunction() {
    RemoteFunctionConfig config = c.getRemoteFunction();
    Assert.assertNotNull(config);
    Assert.assertEquals("euWest1", config.id);

    c.remoteFunctions.remove("euWest1");
    config = c.getRemoteFunction();
    Assert.assertNotNull(config);
    Assert.assertEquals("usEast1", config.id);

    c.remoteFunctions.remove("usEast1");
    Assert.assertThrows("No matching remote function is defined for connector with ID test and remote-function pool ID euWest1 and none of the remote functions is flagged as defaultConfig.", RuntimeException.class, () -> c.getRemoteFunction());

    config = c.remoteFunctions.remove("euCentral1");
    Assert.assertThrows("No remote functions are defined for connector with ID test", RuntimeException.class, () -> c.getRemoteFunction());

    c.setRemoteFunction(config);
    config = c.getRemoteFunction();
    Assert.assertNotNull(config);
    Assert.assertEquals("euCentral1", config.id);
  }
}
