package com.here.naksha.storage.http.connector.integration.tests;

import com.here.naksha.storage.http.connector.integration.utils.DataHub;
import com.here.naksha.storage.http.connector.integration.utils.Naksha;
import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static com.here.naksha.storage.http.connector.integration.utils.Commons.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TileTest {
  private static final int HALF_TILE_SIZE_MARGIN = 128;

  // Quadkey 30000:
  //      0 N
  //       |
  // 0 W -- -- 11.250 E
  //       |
  //   11.1784... S
  private static final int QUADKEY = 30000;

  @BeforeEach
  void rmFeatures() {
    rmAllFeatures();
  }

  @Test
  void testTranslationToBBox() {

    DataHub.createFeatureFromJsonTemplateFile( // clearly inside
      "tile/feature_template.json",
      "1",
      "[3, -3, 0], [0, -3, 0], [0, 0, 0], [3, 0, 0], [3, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // clearly outside
      "tile/feature_template.json",
      "2",
      "[-3, -3, 0], [-1, -3, 0], [-1, 0, 0], [-3, 0, 0], [-3, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // inside, east edge (11.25E)
      "tile/feature_template.json",
      "3",
      "[11.25, -3, 0], [12, -3, 0], [12, 0, 0], [11.25, 0, 0], [11.25, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // outside, east edge (11.25E)
      "tile/feature_template.json",
      "4",
      "[11.251, -3, 0], [12, -3, 0], [12, 0, 0], [11.251, 0, 0], [11.251, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // inside, west edge (0W)
      "tile/feature_template.json",
      "5",
      "[-3, -3, 0], [0, -3, 0], [0, 0, 0], [-3, 0, 0], [-3, -3, 0]"
    );


    DataHub.createFeatureFromJsonTemplateFile( // outside, west edge (0W)
      "tile/feature_template.json",
      "6",
      "[-3, -3, 0], [-0.01, -3, 0], [-0.01, 0, 0], [-3, 0, 0], [-3, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // inside, north edge (0N)
      "tile/feature_template.json",
      "7",
      "[0, 0, 0], [3, 0, 0], [3, 3, 0], [0, 3, 0], [0, 0, 0]"
    );


    DataHub.createFeatureFromJsonTemplateFile( // outside, north edge (0N)
      "tile/feature_template.json",
      "8",
      "[0, 0.01, 0], [3, 0.01, 0], [3, 3, 0], [0, 3, 0], [0, 0.01, 0]"
    );

    String path = "tile/quadkey/" + QUADKEY;
    Response dhResponse = DataHub.request().get(path);
    Response nResponse = Naksha.request().get(path);
    assertTrue(responseHasExactShortIds(List.of("1", "3", "5", "7"), dhResponse));
    assertTrue(responseHasExactShortIds(List.of("1", "3", "5", "7"), nResponse));
  }

  @Test
  void testPadding() {

    DataHub.createFeatureFromJsonTemplateFile( // clearly inside
      "tile/feature_template.json",
      "1",
      "[3, -3, 0], [0, -3, 0], [0, 0, 0], [3, 0, 0], [3, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // clearly outside
      "tile/feature_template.json",
      "2",
      "[-13, -3, 0], [-11, -3, 0], [-11, 0, 0], [-13, 0, 0], [-13, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // inside, east edge (16.875 E = 11.25E tile + 5.625E margin)
      "tile/feature_template.json",
      "3",
      "[16.875, -3, 0], [17, -3, 0], [17, 0, 0], [16.875, 0, 0], [16.875, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // inside, east edge (16.875 E = 11.25E tile + 5.625E margin)
      "tile/feature_template.json",
      "4",
      "[16.876, -3, 0], [17, -3, 0], [17, 0, 0], [16.876, 0, 0], [16.876, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // inside, west edge (5.625W = 0W tile + 5.625W margin)
      "tile/feature_template.json",
      "5",
      "[-6, -3, 0], [-5.625, -3, 0], [-5.625, 0, 0], [-6, 0, 0], [-6, -3, 0]"
    );


    DataHub.createFeatureFromJsonTemplateFile( // outside, west edge (5.625W = 0W tile + 5.625W margin)
      "tile/feature_template.json",
      "6",
      "[-6, -3, 0], [-5.626, -3, 0], [-5.626, 0, 0], [-6, 0, 0], [-6, -3, 0]"
    );

    DataHub.createFeatureFromJsonTemplateFile( // inside, north edge
      "tile/feature_template.json",
      "7",
      "[0, 5.61598, 0], [3, 5.61598, 0], [3, 6, 0], [0, 6, 0], [0, 5.61598, 0]"
    );


    DataHub.createFeatureFromJsonTemplateFile( // outside, north edge
      "tile/feature_template.json",
      "8",
      "[0, 5.61599, 0], [3, 5.61599, 0], [3, 6, 0], [0, 6, 0], [0, 5.61599, 0]"
    );

    String path = "tile/quadkey/" + QUADKEY + "?margin=" + HALF_TILE_SIZE_MARGIN;

    Response dhResponse = DataHub.request().get(path);
    Response nResponse = Naksha.request().get(path);
    assertSameIds(dhResponse, nResponse);
    assertTrue(responseHasExactShortIds(List.of("1", "3", "5", "7"), dhResponse));
    assertTrue(responseHasExactShortIds(List.of("1", "3", "5", "7"), nResponse));
  }

  @ParameterizedTest
  @ValueSource(strings = {"here", "tms"})
  void testUnsupportedTileType(String unsupportedTileType) {

    DataHub.createFeatureFromJsonTemplateFile( // exemplary feature
      "tile/feature_template.json",
      "1",
      "[3, -3, 0], [0, -3, 0], [0, 0, 0], [3, 0, 0], [3, -3, 0]"
    );

    String pathToUnsupportedTile = "tile/" + unsupportedTileType + "/" + QUADKEY;

    Response response = Naksha.request().get(pathToUnsupportedTile);
    assertEquals("ErrorResponse", response.jsonPath().getString("type"));
    assertEquals("IllegalArgument", response.jsonPath().getString("error"));
  }
}
