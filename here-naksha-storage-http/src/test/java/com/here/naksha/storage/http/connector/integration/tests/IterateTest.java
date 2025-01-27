package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import com.here.naksha.storage.http.connector.integration.utils.Naksha;
import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IterateTest {
  @BeforeEach
  void rmFeatures() {
    rmAllFeatures();
  }

  @Test
  void test() {
    IntStream.rangeClosed(1,5).forEach( i ->
            DataHub.createFeatureFromJsonTemplateFile("iterate/feature_template.json",String.valueOf(i) )
    );

    String path = "iterate";
    Response dhResponse = DataHub.request().get(path);
    Response nResponse = Naksha.request().get(path);
    assertTrue(responseHasExactShortIds(List.of("1","2","3","4","5"), dhResponse));
    assertTrue(responseHasExactShortIds(List.of("1","2","3","4","5"), nResponse));
  }
}
