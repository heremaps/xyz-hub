package com.here.naksha.handler.activitylog.util;

import com.here.naksha.handler.activitylog.ActivityLogComparator;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.Original;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.test.common.FileUtil;
import java.util.List;

public class DatahubSamplesUtil {

  public static final String SAMPLE_SPACE_ID = "SDNujm7h";

  private static final String SAMPLES_DIR = "src/test/resources/dh_samples/";
  private static final String SAMPLES_FILE = "PropSearchForFeatureId.json";

  public static DatahubSample loadDatahubSample() {
    String sampleJson = loadDatahubSampleJson();
    return new DatahubSample(
        historyFeatures(sampleJson),
        activityFeatures(sampleJson)
    );
  }

  private static String loadDatahubSampleJson() {
    return FileUtil.loadFileOrFail(SAMPLES_DIR, SAMPLES_FILE);
  }

  private static List<XyzFeature> historyFeatures(String sampleFeaturesJson) {
    List<XyzFeature> features = activityFeatures(sampleFeaturesJson);
    features.forEach(feature -> {
      String originFeatureId = feature.getProperties().getXyzActivityLog().getId();
      feature.setId(originFeatureId);
      feature.getProperties().setXyzActivityLog(null);
    });
    return features;
  }

  private static List<XyzFeature> activityFeatures(String sampleFeaturesJson) {
    List<XyzFeature> features = featuresFromCollectionJson(sampleFeaturesJson);
    features.forEach(feature -> {
      String originId = feature.getId();
      XyzActivityLog datahubActivityLog = feature.getProperties().getXyzActivityLog();
      XyzNamespace datahubXyzNamespace = feature.getProperties().getXyzNamespace();
      Original datahubOriginal = datahubActivityLog.getOriginal();
      String originAction = datahubActivityLog.getAction();
      String originPuuid = datahubOriginal.getPuuid();
      long updatedAt = datahubOriginal.getUpdatedAt();
      long createdAt = datahubOriginal.getCreatedAt();
      feature.getProperties().getXyzNamespace().setUuid(originId);
      if (originAction.equals("SAVE")) {
        originAction = "CREATE";
        datahubActivityLog.setAction("CREATE");
      }
      if (datahubOriginal.getSpace() == null) {
        datahubOriginal.setSpace(SAMPLE_SPACE_ID);
      }
      datahubXyzNamespace.setAction(EXyzAction.get(EXyzAction.class, originAction));
      datahubXyzNamespace.setPuuid(originPuuid);
      datahubXyzNamespace.setUpdatedAt(updatedAt);
      datahubXyzNamespace.setCreatedAt(createdAt);
    });
    features.sort(new ActivityLogComparator());
    return features;
  }

  private static List<XyzFeature> featuresFromCollectionJson(String featuresCollectionJson) {
    XyzFeatureCollection collection = JsonSerializable.deserialize(featuresCollectionJson, XyzFeatureCollection.class);
    return collection.getFeatures();
  }

  public record DatahubSample(List<XyzFeature> historyFeatures, List<XyzFeature> activityFeatures) {

  }
}
