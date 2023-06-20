package com.here.naksha.handler.activitylog;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.here.mapcreator.ext.naksha.PsqlConfig;
import com.here.mapcreator.ext.naksha.PsqlConfigBuilder;
import com.here.mapcreator.ext.naksha.PsqlDataSource;
import com.here.naksha.handler.activitylog.ActivityLogDBWriter;
import com.here.naksha.handler.activitylog.ActivityLogHandler;
import com.here.xyz.IoEventPipeline;
import com.here.xyz.models.Typed;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.namespaces.Original;
import com.here.xyz.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.xyz.models.hub.plugins.EventHandler;
import com.here.xyz.models.payload.XyzResponse;
import com.here.xyz.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.models.payload.responses.ErrorResponse;
import com.here.xyz.models.payload.responses.XyzError;
import com.here.xyz.util.IoHelp;
import com.here.xyz.util.json.JsonSerializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

class ActivityLogHandlerTest {
    private static final String APP_NAME = "xyz-hub.test";
    private static final String CONFIG_FILENAME_LOCALHOST = "activity_log_localhost_db_config.json";
    private static final String CONFIG_FILENAME_ACTIVITY_LOG = "activity_log_DB_config.json";

    static EventHandler eventHandler;
    static IoEventPipeline eventPipeline;

    @BeforeAll
    static void setup() throws Exception {
        eventHandler = new EventHandler("test:activity-log", ActivityLogHandler.class);
        eventPipeline = new IoEventPipeline();
        eventPipeline.addEventHandler(eventHandler.newInstance());
    }

    @Test
    void test_GetFeaturesById() throws IOException {
        final GetFeaturesByIdEvent event = new GetFeaturesByIdEvent();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        // XyzSerializable.deserialize(IoEventPipeline.readResource(""))
        eventPipeline.sendEvent(IoHelp.openResource("testevent.json"), out);
        final XyzResponse response = JsonSerializable.deserialize(out.toByteArray(), XyzResponse.class);
        assertNotNull(response);
        assertInstanceOf(ErrorResponse.class, response);
        final ErrorResponse errorResponse = (ErrorResponse) response;
        assertSame(XyzError.NOT_IMPLEMENTED, errorResponse.getError());
    }

    @Test
    void test_fromActivityLog() throws IOException {
        final Feature feature =
                JsonSerializable.deserialize(IoHelp.openResource("activity_log_feature.json"), Feature.class);
        assertNotNull(feature);
        assertNotNull(feature.getProperties());
        assertNotNull(feature.getProperties().getXyzNamespace());
        assertNotNull(feature.getProperties().getXyzActivityLog());
        assertNotNull(feature.getProperties().getXyzActivityLog().getOriginal());
        final String xyzNamespacePuuid =
                feature.getProperties().getXyzActivityLog().getOriginal().getPuuid();
        final String xyzNamespaceMuuid =
                feature.getProperties().getXyzActivityLog().getOriginal().getMuuid();
        final String xyzNamespaceSpace =
                feature.getProperties().getXyzActivityLog().getOriginal().getSpace();
        final long xyzNamespaceCreatedAt =
                feature.getProperties().getXyzActivityLog().getOriginal().getCreatedAt();
        final long xyzNamespaceUpdatedAt =
                feature.getProperties().getXyzActivityLog().getOriginal().getUpdatedAt();
        ActivityLogHandler.fromActivityLogFormat(feature);
        assertSame(xyzNamespacePuuid, feature.getProperties().getXyzNamespace().getPuuid());
        assertSame(xyzNamespaceMuuid, feature.getProperties().getXyzNamespace().getMuuid());
        assertSame(xyzNamespaceSpace, feature.getProperties().getXyzNamespace().getSpace());
        assertEquals(
                xyzNamespaceCreatedAt, feature.getProperties().getXyzNamespace().getCreatedAt());
        assertEquals(
                xyzNamespaceUpdatedAt, feature.getProperties().getXyzNamespace().getUpdatedAt());
        assertNull(feature.getProperties().getXyzActivityLog());
    }

    @Test
    void test_fromActivityLogPartial() throws IOException {
        final Feature feature =
                JsonSerializable.deserialize(IoHelp.openResource("activity_log_partial.json"), Feature.class);
        assertNotNull(feature);
        assertNotNull(feature.getProperties());
        assertNotNull(feature.getProperties().getXyzNamespace());
        assertNotNull(feature.getProperties().getXyzActivityLog());
        assertNotNull(feature.getProperties().getXyzActivityLog().getOriginal());
        final String xyzNamespacePuuid =
                feature.getProperties().getXyzActivityLog().getOriginal().getPuuid();
        final String xyzNamespaceMuuid =
                feature.getProperties().getXyzActivityLog().getOriginal().getMuuid();
        final String xyzNamespaceSpace =
                feature.getProperties().getXyzActivityLog().getOriginal().getSpace();
        final long xyzNamespaceCreatedAt =
                feature.getProperties().getXyzActivityLog().getOriginal().getCreatedAt();
        final long xyzNamespaceUpdatedAt =
                feature.getProperties().getXyzActivityLog().getOriginal().getUpdatedAt();
        ActivityLogHandler.fromActivityLogFormat(feature);
        assertSame(xyzNamespacePuuid, feature.getProperties().getXyzNamespace().getPuuid());
        assertSame(xyzNamespaceMuuid, feature.getProperties().getXyzNamespace().getMuuid());
        assertSame(xyzNamespaceSpace, feature.getProperties().getXyzNamespace().getSpace());
        assertEquals(
                xyzNamespaceCreatedAt, feature.getProperties().getXyzNamespace().getCreatedAt());
        assertEquals(
                xyzNamespaceUpdatedAt, feature.getProperties().getXyzNamespace().getUpdatedAt());
        assertNull(feature.getProperties().getXyzActivityLog());
    }

    @Test
    void test_toActivityLog() throws IOException {
        final Feature feature =
                JsonSerializable.deserialize(IoHelp.openResource("naksha_feature_1.json"), Feature.class);
        final Feature oldFeature =
                JsonSerializable.deserialize(IoHelp.openResource("naksha_feature_2.json"), Feature.class);
        assertNotNull(feature);
        assertNotNull(oldFeature);
        ActivityLogHandler.toActivityLogFormat(feature, oldFeature);
        final Original original = feature.getProperties().getXyzActivityLog().getOriginal();
        final XyzNamespace xyzNameSpace = feature.getProperties().getXyzNamespace();
        final XyzActivityLog xyzActivityLog = feature.getProperties().getXyzActivityLog();
        assertNotNull(original);
        assertNotNull(xyzNameSpace);
        assertNotNull(xyzActivityLog);
        assertNotNull(xyzActivityLog.getDiff());
        assertSame(original.getPuuid(), xyzNameSpace.getPuuid());
        assertSame(original.getMuuid(), xyzNameSpace.getMuuid());
        assertSame(original.getSpace(), xyzNameSpace.getSpace());
        assertSame(original.getSpace(), xyzNameSpace.getSpace());
        assertEquals(original.getCreatedAt(), xyzNameSpace.getCreatedAt());
        assertEquals(xyzActivityLog.getAction(), xyzNameSpace.rawAction());
    }

    private boolean isAllTestEnvVarsSet() {
        return System.getenv("REMOTE_ACTIVITY_LOG_DB") != null
                && System.getenv("REMOTE_ACTIVITY_LOG_DB").length() > 0
                && System.getenv("LOCAL_ACTIVITY_LOG_DB") != null
                && System.getenv("LOCAL_ACTIVITY_LOG_DB").length() > 0;
    }

    @Test
    @EnabledIf("isAllTestEnvVarsSet")
    void test_connectToDb() throws Exception {
        final PsqlConfig remoteActivityLogDbConfig = new PsqlConfigBuilder()
                .withAppName("Naksha-Psql-Test")
                .parseUrl(System.getenv("REMOTE_ACTIVITY_LOG_DB"))
                .build();
        final PsqlConfig localActivityLogDbConfig = new PsqlConfigBuilder()
                .withAppName("Naksha-Psql-Test")
                .parseUrl(System.getenv("LOCAL_ACTIVITY_LOG_DB"))
                .build();
        final PsqlDataSource dataSourceLocalhost = new PsqlDataSource(localActivityLogDbConfig);
        final PsqlDataSource dataSourceActivityLog = new PsqlDataSource(remoteActivityLogDbConfig);
        ActivityLogDBWriter.fromActicityLogDBToFeature(dataSourceLocalhost, dataSourceActivityLog, "RnxiONGZ", 10);
    }

    @Test
    void test_deserialization() throws IOException {
        final Feature feature =
                JsonSerializable.deserialize(IoHelp.openResource("naksha_feature_1.json"), Feature.class);
        final Typed raw = JsonSerializable.deserialize(IoHelp.openResource("naksha_feature_1.json"), Typed.class);
        final JsonNode raw1 =
                JsonSerializable.deserialize(IoHelp.openResource("naksha_feature_1.json"), JsonNode.class);
        final String raw3 = JsonSerializable.serialize(feature);
        assertInstanceOf(Feature.class, raw);
    }

    @Test
    void test_sqlQueryBuilder() throws IOException {
        List<String> listFeatures = Arrays.asList("sup1", "sup2", "sup3");
        List<String> listGeos = Arrays.asList("geo1", "geo2", "geo3");
        List<Integer> listi = Arrays.asList(1, 2, 3);
        String query = ActivityLogDBWriter.sqlQueryInsertConvertedFeatures(listFeatures, "activity", listGeos, listi);
        assertEquals(
                query,
                "INSERT INTO activity.\"Features_Original_Format\"(jsondata,geo,i) VALUES ('sup1', 'geo1', 1),('sup2', 'geo2', 2),('sup3', 'geo3', 3)");
    }
}
