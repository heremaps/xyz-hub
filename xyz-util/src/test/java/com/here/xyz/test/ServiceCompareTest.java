package com.here.xyz.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


import java.net.http.HttpRequest;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.here.xyz.util.Random.randomAlpha;

//TODO: Remove before merging to master
@Disabled
public class ServiceCompareTest {
    private static final String service1 = "http://localhost:8080/hub";
    private static final String service2 = "..";
    private static final String service2_maintenance = "...";

    protected String SPACE_ID = getClass().getSimpleName() + "_" + randomAlpha(5);

    @AfterEach
    public void tearDown() throws XyzWebClient.WebClientException {
        deleteSpaces();
    }

    /** Without history */
    @Test
    public void testEmptySpaceWithoutHistory() throws XyzWebClient.WebClientException {
        createSpaces(1);
        compareStatistics();
    }

    @Test
    public void testSpaceWithoutHistoryWithOneWrite() throws XyzWebClient.WebClientException {
        createSpaces(1);
        writeFeatureWithId("feature1");
        //TODO: Fail on service1 there we are delivering the correct minVersion
        compareStatistics();
    }

    @Test
    public void testSpaceWithoutHistoryWithMultipleWrites() throws XyzWebClient.WebClientException {
        createSpaces(1);
        for (int i = 0; i < 15; i++) {
            writeFeatureWithId("feature1");
        }
        //TODO: Fails on service1 there we are delivering the correct minVersion
        compareStatistics();
    }
    /************************************ With history */
    @Test
    public void testEmptySpaceWitHistory() throws XyzWebClient.WebClientException {
        createSpaces(10);
        compareStatistics();
    }

    @Test
    public void testSpaceWithHistoryWithOneWrite() throws XyzWebClient.WebClientException, JsonProcessingException {
        createSpaces(10);
        writeFeatureWithId("feature1");
        compareFeatureHistory(0, false);
        compareFeatureHistory(1, true);
        compareBothStatistics();
    }

    @Test
    public void testSpaceWithHistoryWithMultipleWrites() throws XyzWebClient.WebClientException, JsonProcessingException {
        createSpaces(10);

        //purge in service1 happens automatically - version % 10 .. so we need at least 2x10 versions
        for (int i = 0; i < 20; i++) {
            writeFeatureWithId("feature1");
        }
        purgeSpace(-1);
        checkSpaceMinVersion();

        //purge has happened, so we expect 20-10=10 as lowest version
        compareFeatureHistory(21, true);
        compareFeatureHistory(20, true);
        compareFeatureHistory(11, true);
        //TODO: check if 10 should be available or not
        compareFeatureHistory(10, false);

        compareBothStatistics();
    }

    @Test
    public void testSpaceWithHistoryWithMultipleWritesAndUserTag() throws XyzWebClient.WebClientException, JsonProcessingException {
        long tagVersion = 3;

        createSpaces(10);
        for (int i = 0; i < 5; i++) {
            writeFeatureWithId("feature1");
        }

        //If tag is created later a version purge has happened in between
        createTag(tagVersion,false);

        //purge in service1 happens automatically - version % 10 .. so we need at least 2x10 versions
        for (int i = 0; i < 20; i++) {
            writeFeatureWithId("feature1");
        }
        purgeSpace(tagVersion);
        checkSpaceMinVersion();

        //purge has happened, so we expect 25-10=15 as lowest version - in db also previous version exists (cause of tag)
        compareFeatureHistory(15, true);
        compareFeatureHistory(14, false);

        //version 3 should be available, cause of tag
        checkChangeset(3, false, true, false);
        //expect one on available version
        compareChangesets(0,3,1);

        //expect three on available versions
        compareChangesets(0,5,3);

        compareBothStatistics();
    }


    @Test
    public void testSpaceWithHistoryWithMultipleWritesAndSystemTag() throws XyzWebClient.WebClientException, JsonProcessingException {
        long tagVersion = 5;

        createSpaces(10);
        for (int i = 0; i < 5; i++) {
            writeFeatureWithId("feature1");
        }

        createTag(tagVersion,true);
        //purge in service1 happens automatically - version % 10 .. so we need at least 2x10 versions
        for (int i = 0; i < 20; i++) {
            writeFeatureWithId("feature1");
        }
        purgeSpace(tagVersion);
        //purge in service 2 happens automatically - version % 10 .. so we need at least 2x10 versions
        checkSpaceMinVersion();

        //purge has happened, so we expect 25-10=15 as lowest version - in db also previous version exists (cause of tag)
        compareFeatureHistory(15, true);
        compareFeatureHistory(14, false);

        //version 3 should be available
        checkIfChangesetNotExists(3);

        //version 5 should be available, cause of tag
        checkChangeset(5, false, true, false);

        //expect one on available version
        compareChangesets(0,6,2);

        //expect three on available versions
        compareChangesets(0,8,4);

        compareBothStatistics();
    }

    @Test
    public void testSpaceWithHistoryWithMultipleWritesAndSystemTagAndUserPurge() throws XyzWebClient.WebClientException, JsonProcessingException {
        long tagVersion = 5;

        createSpaces(10);
        for (int i = 0; i < 5; i++) {
            writeFeatureWithId("feature1");
        }

        createTag(tagVersion,true);

        for (int i = 0; i < 20; i++) {
            writeFeatureWithId("feature1");
        }

        //TODO: current behavior it fails on service2 - which is wrong.
        deleteChangesets(7, false, true);

        //Trigger Lambda does this in v2 - in v2 this happens automatically
        purgeSpace(Math.min(tagVersion, 7));

        checkIfChangesetNotExists(4);
        checkChangeset(5, false, true, false);
        compareChangesets(0,7,3);

        //TODO: Fails currently because in service1 we are respecting a user purge without deleting the changesets.
        compareBothStatistics();
    }

    @Test
    public void testSpaceWithHistoryWithMultipleWritesAndUserTagAndUserPurge() throws XyzWebClient.WebClientException, JsonProcessingException {
        long tagVersion = 5;

        createSpaces(10);
        for (int i = 0; i < 5; i++) {
            writeFeatureWithId("feature1");
        }

        createTag(tagVersion,false);

        for (int i = 0; i < 20; i++) {
            writeFeatureWithId("feature1");
        }

        deleteChangesets(7, true, true);

        purgeSpace(tagVersion);

        checkIfChangesetNotExists(4);
        checkChangeset(5, false, true, false);
        compareChangesets(0,7,3);

        compareBothStatistics();
    }

    @Test
    public void testSpaceWithHistoryWithMultipleWritesAndSystemAndUserTagAndUserPurge() throws XyzWebClient.WebClientException, JsonProcessingException {
        long userTagVersion = 3;
        long systemTagVersion = 5;

        createSpaces(10);
        for (int i = 0; i < 5; i++) {
            writeFeatureWithId("feature1");
        }

        createTag("tag1",userTagVersion,false);
        createTag("tag2",userTagVersion,true);
        checkChangeset(2, false, true, false);

        for (int i = 0; i < 20; i++) {
            writeFeatureWithId("feature1");
        }
        //would fail on service1, because there was a version purge in between
        //checkChangeset(2, false, true, false);
        //TODO: service1 allow purge because systemTag is not known
        deleteChangesets(7, false, true);

        purgeSpace(Math.min(userTagVersion, systemTagVersion));

        checkIfChangesetNotExists(2);
        checkChangeset(5, false, true, false);
        compareChangesets(0,7,5);

        compareFeatureHistory(5, false);

        //TODO: Fails currently because in service1 we are respecting a user purge without deleting the changesets.
        compareBothStatistics();
    }

    @Test
    public void testSpaceWithHistoryWithMultipleWritesAndUserPurge() throws XyzWebClient.WebClientException, JsonProcessingException {

        createSpaces(10);
        for (int i = 0; i < 25; i++) {
            writeFeatureWithId("feature1");
        }

        deleteChangesets(20, false, false);
        purgeSpace(-1);

        checkIfChangesetNotExists(20);
        checkChangeset(21, false, true, false);
        compareChangesets(0,25,6);

        compareBothStatistics();
    }


    //***************************************************************************/

    private void writeFeatureWithId(String id) throws XyzWebClient.WebClientException {
        writeFeatures(new FeatureCollection()
                .withFeatures(Arrays.asList(
                        new com.here.xyz.models.geojson.implementation.Feature()
                                .withProperties(new Properties().with("val", UUID.randomUUID()))
                                .withId(id)
                                .withGeometry(new Point().withCoordinates(new PointCoordinates(1,1)))
                )));
    }

    private void createSpaces(int versionsToKeep) throws XyzWebClient.WebClientException {
        System.out.println("Creating spaces with id: " + SPACE_ID + "...");
        Space space = new Space()
                .withTitle("Test Space for Service Comparison")
                .withId(SPACE_ID)
                .withVersionsToKeep(versionsToKeep);

        HubWebClient.getInstance(service1)
                .createSpace(space);

        HubWebClient.getInstance(service2)
                .createSpace(space);
    }

    private void deleteSpaces() throws XyzWebClient.WebClientException {
        System.out.println("Deleting spaces with id: " + SPACE_ID + "...");
        HubWebClient.getInstance(service1)
                .deleteSpace(SPACE_ID);
        HubWebClient.getInstance(service2)
                .deleteSpace(SPACE_ID);
    }

    private void deleteChangesets(long version, boolean expectErrorS1, boolean expectErrorS2) throws XyzWebClient.WebClientException {
        int errorCount = 0;
        try{
            HubWebClient.getInstance(service1)
                    .deleteChangesets(SPACE_ID, version);
        }catch (XyzWebClient.WebClientException e){
            if(!expectErrorS1)
                Assertions.fail("Unexpected error on service1", e);
            errorCount ++;
        }
        if(expectErrorS1)
            Assertions.assertEquals(1, errorCount, "expecting error on service1");

        try{
            HubWebClient.getInstance(service2)
                    .deleteChangesets(SPACE_ID, version);
        }catch (XyzWebClient.WebClientException e){
            if(!expectErrorS2)
                Assertions.fail("Unexpected error on service2", e);
            errorCount ++;
        }

        if(expectErrorS1 && expectErrorS2)
            Assertions.assertEquals(2, errorCount, "expecting two errors");
    }

    private void createTag(
            long version, boolean isSystemTag) throws XyzWebClient.WebClientException {
        createTag("tag1", version, isSystemTag);
    }

    private void createTag(String tagName, long version, boolean isSystemTag) throws XyzWebClient.WebClientException {
        Tag tag1 = new Tag()
                .withId(tagName)
                .withVersion(version)
                .withSystem(isSystemTag);

        HubWebClient.getInstance(service1)
                .postTag(SPACE_ID, tag1);
        HubWebClient.getInstance(service2)
                .postTag(SPACE_ID, tag1);
    }

    private void writeFeatures(FeatureCollection fc) throws XyzWebClient.WebClientException {
        HubWebClient.getInstance(service1)
                .putFeaturesWithoutResponse(SPACE_ID, fc);
        HubWebClient.getInstance(service2)
                .putFeaturesWithoutResponse(SPACE_ID, fc);
    }

    private void compareBothStatistics() throws XyzWebClient.WebClientException {
        compareStatistics();
        compareChangesetStatistics();
    }

    private void checkSpaceMinVersion() throws XyzWebClient.WebClientException {
        Space space1 = HubWebClient.getInstance(service1)
                .loadSpace(SPACE_ID);
        Space space2 = HubWebClient.getInstance(service2)
                .loadSpace(SPACE_ID);
        Assertions.assertEquals(space1.getMinVersion(), space2.getMinVersion(),"minVersion should be equal");
    }

    private void compareStatistics()
            throws XyzWebClient.WebClientException {

        StatisticsResponse statisticsResponse1 = HubWebClient.getInstance(service1)
                .loadSpaceStatistics(SPACE_ID, ContextAwareEvent.SpaceContext.DEFAULT, true, false);

        StatisticsResponse statisticsResponse2 = HubWebClient.getInstance(service2)
                .loadSpaceStatistics(SPACE_ID, ContextAwareEvent.SpaceContext.DEFAULT,true, false);

        Assertions.assertEquals(statisticsResponse1.getMaxVersion().getValue(),
                statisticsResponse2.getMaxVersion().getValue(), "maxVersion should be equal");
        Assertions.assertEquals(statisticsResponse1.getMaxVersion().getEstimated(),
                statisticsResponse2.getMaxVersion().getEstimated(), "maxVersion estimation should be equal");

        Assertions.assertEquals(statisticsResponse1.getMinVersion().getValue(),
                statisticsResponse2.getMinVersion().getValue(), "minVersion should be equal");
        Assertions.assertEquals(statisticsResponse1.getMinVersion().getEstimated(),
                statisticsResponse2.getMinVersion().getEstimated(),"minVersion estimation should be equal");
    }

    private void compareChangesetStatistics()
            throws XyzWebClient.WebClientException {

        ChangesetsStatisticsResponse csStatisticsResponse1 = HubWebClient.getInstance(service1)
                .loadSpaceChangesetStatistics(SPACE_ID);

        ChangesetsStatisticsResponse csStatisticsResponse2 = HubWebClient.getInstance(service2)
                .loadSpaceChangesetStatistics(SPACE_ID);

        Assertions.assertEquals(csStatisticsResponse1.getMaxVersion(),
                csStatisticsResponse2.getMaxVersion(),"maxVersion should be equal");

        Assertions.assertEquals(csStatisticsResponse1.getMinTagVersion() != null ?
                        csStatisticsResponse1.getMinTagVersion() : null,
                csStatisticsResponse1.getMinTagVersion() != null ?
                        csStatisticsResponse2.getMinTagVersion() : null, "minTagVersion should be equal");

        Assertions.assertEquals(csStatisticsResponse1.getMinVersion(),
                csStatisticsResponse2.getMinVersion(), "minVersion should be equal");

    }

    private void checkIfChangesetNotExists(long version) throws JsonProcessingException, XyzWebClient.WebClientException {
        checkChangeset(version, false, false, false);
    }

    private void checkChangeset(long version, boolean insertsShouldExist, boolean updatesShouldExist,
                                boolean deletesShouldExist)
            throws XyzWebClient.WebClientException, JsonProcessingException {

        Changeset changesetResponse1 = null;
        Changeset changesetResponse2 = null;

        try{
            changesetResponse1 = HubWebClient.getInstance(service1)
                    .getChangeset(SPACE_ID, version);
        }catch (XyzWebClient.WebClientException e){
            if(!insertsShouldExist && !updatesShouldExist && !deletesShouldExist)
                Assertions.assertEquals(404, ((XyzWebClient.ErrorResponseException)e).getStatusCode());
        }

        try{
            changesetResponse2 = HubWebClient.getInstance(service2)
                    .getChangeset(SPACE_ID, version);
        }catch (XyzWebClient.WebClientException e){
            if(!insertsShouldExist && !updatesShouldExist && !deletesShouldExist)
                Assertions.assertEquals(404, ((XyzWebClient.ErrorResponseException)e).getStatusCode());
        }

        if(insertsShouldExist || updatesShouldExist || deletesShouldExist) {
            Assertions.assertEquals(insertsShouldExist, !changesetResponse1.getInserted().getFeatures().isEmpty(),
                    "inserts not match on service1");
            Assertions.assertEquals(insertsShouldExist, !changesetResponse2.getInserted().getFeatures().isEmpty(),
                    "inserts not match on service2");
            Assertions.assertEquals(updatesShouldExist, !changesetResponse1.getUpdated().getFeatures().isEmpty(),
                    "updates not match on service1");
            Assertions.assertEquals(updatesShouldExist, !changesetResponse2.getUpdated().getFeatures().isEmpty(),
                    "updates not match on service2");
            Assertions.assertEquals(deletesShouldExist, !changesetResponse1.getDeleted().getFeatures().isEmpty(),
                    "deletes not match on service1");
            Assertions.assertEquals(deletesShouldExist, !changesetResponse2.getDeleted().getFeatures().isEmpty(),
                    "deletes not match on service2");

            Assertions.assertEquals(changesetResponse1.getInserted().getFeatures().size(),
                    changesetResponse2.getInserted().getFeatures().size(), "inserted should be equal");
            Assertions.assertEquals(changesetResponse1.getDeleted().getFeatures().size(),
                    changesetResponse2.getDeleted().getFeatures().size(), "deleted should be equal");
            Assertions.assertEquals(changesetResponse1.getUpdated().getFeatures().size(),
                    changesetResponse2.getUpdated().getFeatures().size(), "updated should be equal");
        }
    }

    private void compareChangesets(long startVersion, long endVersion, int expectedAmountOfVersions)
            throws XyzWebClient.WebClientException, JsonProcessingException {

        ChangesetCollection changesetCollectionResponse1 = HubWebClient.getInstance(service1)
                .getChangesets(SPACE_ID, startVersion, endVersion);

        ChangesetCollection changesetCollectionResponse2 = HubWebClient.getInstance(service2)
                .getChangesets(SPACE_ID, startVersion, endVersion);

        Assertions.assertEquals(expectedAmountOfVersions, changesetCollectionResponse1.getVersions().size());
        Assertions.assertEquals(expectedAmountOfVersions, changesetCollectionResponse2.getVersions().size());
    }

    private void compareFeatureHistory(long version, boolean expectFeature) throws XyzWebClient.WebClientException, JsonProcessingException {
        String path = "search?version="+version;
        FeatureCollection featureCollection1 = HubWebClient.getInstance(service1)
                .customReadFeaturesQuery(SPACE_ID, path);

        FeatureCollection featureCollection2 = HubWebClient.getInstance(service2)
                .customReadFeaturesQuery(SPACE_ID, path);

        if(expectFeature){
            Assertions.assertNotEquals(0, featureCollection1.getFeatures().size(),
                    "FeatureCollection on service1 should not be empty");
            Assertions.assertNotEquals(0, featureCollection2.getFeatures().size(),
                    "FeatureCollection on service2 should not be empty");
        }else {
            Assertions.assertEquals(0, featureCollection1.getFeatures().size(),
                    "FeatureCollection on service1 should be empty");
            Assertions.assertEquals(0, featureCollection2.getFeatures().size(),
                    "FeatureCollection on service2 should not be empty");
        }
    }

    private void purgeSpace(long minTagVersion) throws XyzWebClient.WebClientException {
        MaintenanceClient.getInstance(service2_maintenance)
                .purgeSpace(SPACE_ID, minTagVersion);
    }

    public static class MaintenanceClient extends HubWebClient{
        private static Map<InstanceKey, MaintenanceClient> instances = new ConcurrentHashMap<>();

        public MaintenanceClient(String baseUrl, Map<String, String> extraHeaders) {
            super(baseUrl, null);
        }

        public static MaintenanceClient getInstance(String baseUrl) {
            return getInstance(baseUrl, null);
        }

        public static MaintenanceClient getInstance(String baseUrl, Map<String, String> extraHeaders) {
            InstanceKey key = new InstanceKey(baseUrl, extraHeaders);
            if (!instances.containsKey(key))
                instances.put(key, new MaintenanceClient(baseUrl, extraHeaders));
            return instances.get(key);
        }

        //https://iml-http-connector.prd.idprd.aws.in.here.com/psql/maintain/spaces/ServiceCompareTest_ajjgs/purge?minTagVersion=5' \
        public void purgeSpace(String spaceId, long minTagVersion) throws XyzWebClient.WebClientException {
            request(HttpRequest.newBuilder()
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .uri(uri("/maintain/spaces/" + spaceId + "/purge" + (minTagVersion != -1L ?
                            "?minTagVersion="+minTagVersion : ""))));
        }
    }
}
