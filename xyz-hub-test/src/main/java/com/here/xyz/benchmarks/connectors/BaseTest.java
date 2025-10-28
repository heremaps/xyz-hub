package com.here.xyz.benchmarks.connectors;

import com.here.xyz.benchmarks.tools.PerformanceTestHelper;
import com.here.xyz.psql.NLConnector;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.createDBSettings;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.createSpace;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.deleteSpace;
import static com.here.xyz.benchmarks.tools.PerformanceTestHelper.initConnector;

@BenchmarkMode(Mode.Throughput)

@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)  // warmup phase
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS) // actual measurement
@Threads(20)
public class BaseTest {
    protected static final int OPERATIONS_PER_INVOCATION = 100;

    @Param({"100"})
    public static int BATCH_SIZE;

    @Param({"100"})
    public static int PAYLOAD_BYTES;

    @Param({"localhost"})
    public static String PG_HOST;

    @Param({"postgres"})
    public static String PG_USER;

    @Param({"postgres"})
    public static String PG_DB;

    @Param({"password"})
    public static String PG_PASSWORD;

    @Param({"40"})
    public static int PG_MAX_POOLSIZE;

    @Param({""})
    public static String SPACE_ID;

    private boolean isSpaceCreation = false;
    protected NLConnector nlConnector;
    protected PSQLXyzConnector psqlConnector;

    protected void setupTest() throws Exception {
        DatabaseSettings dbSettings = createDBSettings(PG_HOST, PG_USER, PG_DB, PG_PASSWORD, PG_MAX_POOLSIZE);
        boolean seedingMode = true;

        nlConnector = initConnector("NLConnector", new NLConnector(), dbSettings, seedingMode);
        psqlConnector = initConnector("PsqlConnector", new PSQLXyzConnector(), dbSettings);

        if(SPACE_ID.equalsIgnoreCase("")) {
            SPACE_ID = Random.randomAlpha(6);
            isSpaceCreation = true;
        }
        createSpace(nlConnector, PerformanceTestHelper.getSpaceName(nlConnector, SPACE_ID));
        createSpace(psqlConnector, PerformanceTestHelper.getSpaceName(psqlConnector, SPACE_ID));
    }


    protected void tearDownTest() throws Exception {
        if(isSpaceCreation) {
            deleteSpace(nlConnector, PerformanceTestHelper.getSpaceName(nlConnector, SPACE_ID));
            deleteSpace(psqlConnector, PerformanceTestHelper.getSpaceName(psqlConnector, SPACE_ID));
        }
    }
}
