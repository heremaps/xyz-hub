package com.here.naksha.handler.activitylog;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.test.common.FileUtil;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;
import org.json.JSONException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

class ActivityLogEnhancerTest {

  private static final String SAMPLES_DIR = "src/test/resources/enhancer_samples/";
  private static final String NEW_FEATURE_JSON = "new_feature.json";
  private static final String OLD_FEATURE_JSON = "old_feature.json";
  private static final String EXPECTED_FEATURE_JSON = "expected_enhanced_feature.json";

  private static final String SPACE_ID = "enhancer_test_space_id";


  @ParameterizedTest
  @MethodSource("samples")
  void shouldEnhanceFeatureWithPredecessor(String sampleDir, XyzFeature oldFeature, XyzFeature newFeature, String expectedFeatureJson)
      throws JSONException {
    // When
    XyzFeature enhancedFeature = ActivityLogEnhancer.enhanceWithActivityLog(newFeature, oldFeature, SPACE_ID);

    // And
    String enhancedFeatureJson = JsonSerializable.serialize(enhancedFeature);

    // Then
    JSONAssert.assertEquals(
        "Comparison failed for sample: " + sampleDir,
        expectedFeatureJson,
        enhancedFeatureJson,
        JSONCompareMode.LENIENT
    );
  }

  private static Stream<Arguments> samples() {
    return sampleDirs()
        .map(path -> {
          String stringPath = path.toString() + "/";
          return Arguments.arguments(
              path.getFileName().toString(),
              featureFromFile(stringPath, OLD_FEATURE_JSON),
              featureFromFile(stringPath, NEW_FEATURE_JSON),
              FileUtil.loadFileOrFail(stringPath, EXPECTED_FEATURE_JSON)
          );
        });
  }

  private static XyzFeature featureFromFile(String sampleDir, String fileName) {
    return JsonSerializable.deserialize(
        FileUtil.loadFileOrFail(sampleDir, fileName),
        XyzFeature.class
    );
  }

  private static Stream<Path> sampleDirs() {
    Path samplesRoot = Paths.get(SAMPLES_DIR);
    return Arrays.stream(samplesRoot.toFile().listFiles(File::isDirectory)).map(File::toPath);
  }
}