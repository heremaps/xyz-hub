package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.features.StorageCollection;
import com.here.naksha.lib.core.storage.IFeatureWriter;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IMasterTransaction;
import com.here.naksha.lib.core.storage.ModifyFeaturesReq;
import com.here.naksha.lib.core.storage.ModifyFeaturesResp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class PsqlClientTest {

  /** This is mainly an example that you can use when running this test. */
  @SuppressWarnings("unused")
  public static final String TEST_ADMIN_DB =
      "jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test";

  @Test
  @EnabledIf("isAllTestEnvVarsSet")
  public void testInit() throws Exception {
    final PsqlConfig config = new PsqlConfigBuilder()
        .withAppName("Naksha-Psql-Test")
        .parseUrl(System.getenv("TEST_ADMIN_DB"))
        .build();
    try (final IStorage client = new PsqlStorage(config, 0L)) {
      try (final IMasterTransaction tx = client.openMasterTransaction()) {
        final StorageCollection testCollection = new StorageCollection("road");
        tx.createCollection(testCollection);
        final IFeatureWriter<Feature> featureWriter = tx.writeFeatures(Feature.class, testCollection);
        final ModifyFeaturesReq<Feature> req = new ModifyFeaturesReq<>();
        req.insert().add(new Feature("foo"));
        final ModifyFeaturesResp<Feature> resp = featureWriter.modifyFeatures(req);
        tx.commit();
      }
    }
  }

  private boolean isAllTestEnvVarsSet() {
    return System.getenv("TEST_ADMIN_DB") != null
        && System.getenv("TEST_ADMIN_DB").length() > 0;
  }
}
