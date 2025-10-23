package com.here.xyz.benchmarks.connectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.List;

import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.SEEDING_STRATEGY;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.generateRandomFeatureCollection;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.getSpaceName;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.writeFeatureCollectionIntoSpace;

public class WriteTests extends BaseTest{

    @Setup(Level.Trial)
    public void setupTest() throws Exception {
        super.setupTest();
    }

    @TearDown(Level.Trial)
    public void tearDownTest() throws Exception {
        super.tearDownTest();
    }

    @OperationsPerInvocation(OPERATIONS_PER_INVOCATION)
    @Benchmark
    public void testWritesWithNLConnector(BenchmarkState state) throws Exception {
        writeFeatureCollection(nlConnector, state.data);
    }

    @OperationsPerInvocation(OPERATIONS_PER_INVOCATION)
    @Benchmark
    public void testWritesWithPSQLConnector(BenchmarkState state) throws Exception {
        writeFeatureCollection(psqlConnector, state.data);
    }

    //Quadkey: 1202032
    static final private float xmin = 5.625f, ymin = 48.92249926375824f, xmax = 8.4375f, ymax = 50.736455137010644f;

    @State(Scope.Thread)
    public static class BenchmarkState {
        FeatureCollection data;

        @Setup(Level.Iteration)
        public void setUp() throws JsonProcessingException {
            data = generateRandomFeatureCollection(BATCH_SIZE, xmin, ymin, xmax, ymax, PAYLOAD_BYTES, false);
        }
    }

    private void writeFeatureCollection(StorageConnector testConnector, FeatureCollection fc) throws Exception {
        writeFeatureCollectionIntoSpace(testConnector,
                List.of(getSpaceName(testConnector, SPACE_ID)),
                SEEDING_STRATEGY,
                fc
        );
    }

    public static void main(String[] args) throws Exception {
        String[] jmhArgs = {
                ".*WriteTests.testWritesWithNLConnector.*",
               // ".*WriteTests.testWritesWithPSQLConnector.*"
        };
        org.openjdk.jmh.Main.main(jmhArgs);
    }
}
