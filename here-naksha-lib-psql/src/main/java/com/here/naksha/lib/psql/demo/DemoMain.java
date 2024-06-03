/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.psql.demo;

public class DemoMain {

  public static void main(String... args) throws Exception {
    // Create config.
    //    final PsqlStorageConfig config = new PsqlStorageConfig()
    //        .withAppName("Naksha-Psql-Test")
    //        .parseUrl("jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=demo")
    //        .build();
    //    // Get the connection pool (in the background shared between all storages that use the same data-source).
    //    final PsqlStorage storage = new PsqlStorage(config, 0L);
    //    try {
    //      // Connect and initialize the database.
    //      storage.init();
    //
    //      // Create settings for the transaction, we need at least an app-id, optional as well an author.
    //      final ITransactionSettings txDemoApp = storage.createSettings().withAppId("demo_app");
    //
    //      // Start a new write transaction and get a new Jackson Json parser.
    //      try (final PsqlTxWriter tx = storage.openMasterTransaction(txDemoApp);
    //          final Json json = Json.get()) {
    //        // Create the demo collection.
    //        final CollectionInfo demo = tx.createCollection(new CollectionInfo("demo"));
    //        tx.commit();
    //        System.out.println(
    //            json.writer(ViewSerialize.Storage.class, true).writeValueAsString(demo));
    //
    //        // Create a new modify features request that returns the result.
    //        final ModifyFeaturesReq<XyzFeature> request = new ModifyFeaturesReq<>(true);
    //
    //        // Create features and add them for insertion.
    //        final ThreadLocalRandom rand = ThreadLocalRandom.current();
    //        final String prefix = "demo";
    //        final int FEATURES = 30_000;
    //        for (int i = 0; i < FEATURES; i++) {
    //          final XyzFeature feature = new XyzFeature(String.format("%s_%06d", prefix, i));
    //          final double longitude = rand.nextDouble(-180, +180);
    //          final double latitude = rand.nextDouble(-90, +90);
    //          final XyzGeometry geometry = new XyzPoint(longitude, latitude);
    //          feature.setGeometry(geometry);
    //          request.insert().add(feature);
    //        }
    //
    //        // Create a feature writer and write the features.
    //        final long START = System.currentTimeMillis();
    //        final PsqlFeatureWriter<XyzFeature> writer = tx.writeFeatures(XyzFeature.class, demo);
    //        final ModifyFeaturesResp response = writer.modifyFeatures(request);
    //        tx.commit();
    //        final long END = System.currentTimeMillis();
    //
    //        // Debug output
    //        System.out.printf(
    //            "Written %d features in %d milliseconds, %d features per second\n",
    //            FEATURES, END - START, (int) (((double) FEATURES / (double) (END - START)) * 1000));
    //        System.out.println(json.writer(ViewSerialize.Storage.class, true)
    //            .writeValueAsString(response.inserted().get(0)));
    //      }
    //    } finally {
    //      storage.close();
    //    }
  }
}
