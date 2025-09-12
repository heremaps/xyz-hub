package com.here.xyz.benchmarks.connectors;

import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.List;

import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.getSpaceName;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.randomChildQuadkey;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.readFeaturesByRefQuad;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.readFeaturesTile;

public class ReadTests extends BaseTest {
    private FeatureCollection featureCollection = new FeatureCollection();
    private long iterationStart;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        SPACE_ID = "t1";
        super.setupTest();
        iterationStart = System.currentTimeMillis();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        double durationInS = (System.currentTimeMillis() - iterationStart) / 1000d;
        System.out.printf(
                "Features read in this trial: %d in %.2f s => %.2f fps%n",
                featureCollection.getFeatures().size(), durationInS,
                (featureCollection.getFeatures().size() / durationInS)
        );
        super.tearDownTest();
    }

    //############################## READ  #############################

    @Benchmark
    public void testReadByRefQuadWithNLConnector() throws Exception {

        FeatureCollection fc = (FeatureCollection) readFeaturesByRefQuad(nlConnector,  getSpaceName(nlConnector, SPACE_ID),
                randomChildQuadkey("1202032", 12), 30000);
        featureCollection.getFeatures().addAll(fc.getFeatures());
    }

    @Benchmark
    public void testReadByTileWithNLConnector() throws Exception {
        featureCollection.getFeatures().addAll(
                readByTile(nlConnector, "1202032", 12, 30000)
        );
    }

    @Benchmark
    public void testReadByTileWithPSQLConnector() throws Exception {
        featureCollection.getFeatures().addAll(
                readByTile(psqlConnector, "1202032", 12, 30000)
        );
    }

    private List<Feature> readByTile(StorageConnector testConnector, String parentTile, int targetLevel, int limit)
            throws Exception {
        return ((FeatureCollection) readFeaturesTile(testConnector, getSpaceName(testConnector, SPACE_ID),
                randomChildQuadkey(parentTile, targetLevel), limit)).getFeatures();
    }

    public static void main(String[] args) throws Exception {
        String[] jmhArgs = {
                ".*ReadTests.testReadByRefQuadWithNLConnector.*",
                ".*ReadTests.testReadByTileWithNLConnector.*",
                ".*ReadTests.testReadByTileWithPSQLConnector.*"
        };
        org.openjdk.jmh.Main.main(jmhArgs);
    }
}
