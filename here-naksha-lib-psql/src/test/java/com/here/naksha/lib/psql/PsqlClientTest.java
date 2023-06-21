package com.here.naksha.lib.psql;


import com.here.naksha.lib.core.models.hub.StorageCollection;
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
        try (final PsqlStorage client = new PsqlStorage(config, 0L)) {
            try (final PsqlTxWriter transaction = client.startWrite()) {
                final StorageCollection testCollection = new StorageCollection("testID");
                transaction.createCollection(testCollection);
                transaction.commit();
            }
        }
    }

    private boolean isAllTestEnvVarsSet() {
        return System.getenv("TEST_ADMIN_DB") != null
                && System.getenv("TEST_ADMIN_DB").length() > 0;
    }
}
