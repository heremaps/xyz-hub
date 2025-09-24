package com.here.xyz.benchmarks;

import com.here.xyz.benchmarks.tools.PerformanceTestHelper;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.NLConnector;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.createDBSettings;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.getSpaceName;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.initConnector;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.randomChildQuadkey;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.readFeaturesTile;
import static com.here.xyz.events.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;

public class ManualTest {

    private static final Logger logger = LogManager.getLogger();

    private static DatabaseHandler CONNECTOR;
    private static final int THREAD_COUNT = 1;

    public static void main(String[] args) {
        try {
            DatabaseSettings dbSettings = createDBSettings("localhost", "postgres", "postgres","password", 40);
//            CONNECTOR = initConnector("ManualTest", new PSQLXyzConnector(), dbSettings);
            CONNECTOR = initConnector("ManualTest", new NLConnector(), dbSettings);

            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                int threadId = i; // just for logging/debugging
                executor.submit(() -> {
                    System.out.printf("Thread %d starting%n", threadId);
                    try {
                        runBlock();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    System.out.printf("Thread %d finished%n", threadId);
                });
            }

            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
        }catch (Exception e){
            System.err.println(e);
        }
    }

    static private float xmin = 7.0f, ymin = 50.0f, xmax = 7.1f, ymax = 50.1f;

    private static void runBlock() throws Exception {
        String spaceName =  Random.randomAlpha(6);

        try {
            PerformanceTestHelper.createSpace(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName));

//                        PerformanceTestHelper.writeFeatureCollectionIntoSpace(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName),
//                        UpdateStrategy.DEFAULT_UPDATE_STRATEGY,
//                    new FeatureCollection()
//                            .withFeatures(List.of(
//                                    new Feature()
//                                            .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)))
////                                            .withProperties(new Properties().with("refQuad", "12020321220021313201"))
//                                    ,
//                                    new Feature()
//                                            .withGeometry(new Point().withCoordinates(new PointCoordinates(9, 50)))
////                                            .withProperties(new Properties().with("refQuad", "12020321220021313201"))
//                            ))
//            );
            FeatureCollection fc = PerformanceTestHelper.generateRandomFeatureCollection(1000, xmin, ymin, xmax, ymax, 100, false);

            PerformanceTestHelper.writeFeatureCollectionIntoSpace(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName), DEFAULT_UPDATE_STRATEGY, fc);
            logger.info("wrote {} features", fc.getFeatures().size());

            PerformanceTestHelper.writeFeatureCollectionIntoSpace(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName), DEFAULT_UPDATE_STRATEGY, fc);
            logger.info("wrote {} features", fc.getFeatures().size());

            if(CONNECTOR instanceof NLConnector) {
                fc = (FeatureCollection) PerformanceTestHelper.readFeaturesByRefQuad(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName), "1202032", 100);
                logger.info(">>>>> readFeaturesByRefQuad {} features", fc.getFeatures().size());

                fc = (FeatureCollection) PerformanceTestHelper.readFeaturesByRefQuad(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName), "1202032", 100, true);
                logger.info(">>>>> readFeaturesByRefQuadCount {} count", fc.getFeatures().get(0).getProperties().get("count").toString());
            }

            fc = (FeatureCollection) PerformanceTestHelper.readFeaturesTile(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName), "1202032", 30000);
            logger.info(">>>>> readFeaturesTile {} features", fc.getFeatures().size());

            List<String> firstFiveFeatures = fc.getFeatures().stream()
                    .limit(5)
                    .map(Feature::getId)
                    .toList();

            fc = (FeatureCollection) PerformanceTestHelper.readFeaturesByIds(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName), firstFiveFeatures);
            logger.info(">>>>> readFeaturesByIds {} features", fc.getFeatures().size());

            logger.info(">>>>> Deleting feature {} ", firstFiveFeatures.get(0));
            PerformanceTestHelper.deleteFeaturesFromSpace(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName),
                    DEFAULT_UPDATE_STRATEGY, List.of(fc.getFeatures().get(0).getId()));

            fc = (FeatureCollection) PerformanceTestHelper.readFeaturesByIds(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName), firstFiveFeatures);
            logger.info(">>>>> readFeaturesByIds {} features", fc.getFeatures().size());

            PerformanceTestHelper.deleteSpace(CONNECTOR, PerformanceTestHelper.getSpaceName(CONNECTOR, spaceName));

        } catch (Exception e) {
            System.err.println(e);
            //Delete Space
            CONNECTOR.handleEvent(new ModifySpaceEvent()
                    .withSpace(spaceName)
                    .withOperation(ModifySpaceEvent.Operation.DELETE));
        }
    }

    private static void runBlock2() throws Exception {
        PerformanceTestHelper.readFeaturesByRefQuad(CONNECTOR, "test22", "1202032", 100);
    }

}
