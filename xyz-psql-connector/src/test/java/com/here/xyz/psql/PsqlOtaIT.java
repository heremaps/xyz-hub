package com.here.xyz.psql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.OneTimeActionEvent;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

public class PsqlOtaIT extends PSQLAbstractIT {

  //@Test
  public void testOtaEvent() throws Exception {
    OneTimeActionEvent ota = new OneTimeActionEvent()
        .withPhase("test")
        .withInputData(Collections.singletonMap("someKey", "someValue"));
    XyzResponse response = XyzSerializable.deserialize(invokeLambda(ota.serialize()));
    assertNotNull(response);
    assertNoErrorInResponse(response);
    assertTrue(response instanceof SuccessResponse);
  }

}
