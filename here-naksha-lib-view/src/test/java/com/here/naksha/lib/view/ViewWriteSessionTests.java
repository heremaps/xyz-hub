package com.here.naksha.lib.view;

import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPoint;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.psql.PsqlFeatureGenerator;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ViewWriteSessionTests extends PsqlTests {

    static final Logger log = LoggerFactory.getLogger(ViewWriteSessionTests.class);

    final boolean enabled() {
        return true;
    }

    @Override
    final boolean dropInitially() {
        return runTest() && DROP_INITIALLY;
    }
    @Override
    final boolean dropFinally() {
        return runTest() && DROP_FINALLY;
    }

    static final String COLLECTION_0 = "test_view0";
    static final String COLLECTION_1 = "test_view1";


    @Test
    @Order(14)
    @EnabledIf("runTest")
    void createCollection() throws NoCursor {
        assertNotNull(storage);
        assertNotNull(session);
        final WriteXyzCollections request = new WriteXyzCollections();
        request.add(EWriteOp.CREATE, new XyzCollection(COLLECTION_0, false, false, true));
        request.add(EWriteOp.CREATE, new XyzCollection(COLLECTION_1, false, false, true));
        try (final ForwardCursor<XyzCollection, XyzCollectionCodec> cursor =
                     session.execute(request).getXyzCollectionCursor()) {
            assertNotNull(cursor);
            assertTrue(cursor.hasNext());
        } finally {
            session.commit(true);
        }
    }

    @Test
    @Order(15)
    @EnabledIf("runTest")
    void addFeatures() {
        assertNotNull(storage);
        assertNotNull(session);
        PsqlFeatureGenerator fg = new PsqlFeatureGenerator();
        final WriteXyzFeatures requestTest0 = new WriteXyzFeatures(COLLECTION_0);

        final XyzFeature feature = fg.newRandomFeature();
        feature.setGeometry(new XyzPoint(0d, 0d));
        feature.setId("feature_id_view0");
        requestTest0.add(EWriteOp.PUT, feature);

        try {
            session.execute(requestTest0);
        } finally {
            session.commit(true);
        }
    }

    @Test
    @Order(16)
    @EnabledIf("runTest")
    void readAndWrite_UsingViewWriteSession() throws NoCursor {
        assertNotNull(storage);

        ViewLayer layer0 = new ViewLayer(storage, COLLECTION_0);
        ViewLayer layer1 = new ViewLayer(storage, COLLECTION_1);

        ViewLayerCollection viewLayerCollection = new ViewLayerCollection("Layers", layer0, layer1);
        View view = new View(viewLayerCollection);

        try (ViewWriteSession writeSession = view.newWriteSession(nakshaContext, true)) {
            ReadFeatures readRequest = new ReadFeatures();
            readRequest.setPropertyOp(POp.eq(PRef.id(),"feature_id_view0"));
            //Read feature
            final SeekableCursor<XyzFeature, XyzFeatureCodec> cursor = writeSession.execute(readRequest).getXyzSeekableCursor();
            List<XyzFeatureCodec> features = cursor.asList();

            assertEquals(1, features.size());
            assertEquals(0d, features.get(0).getGeometry().getCoordinate().x);

            //Update fetched feature using viewwritesession
            final LayerWriteFeatureRequest writeRequest = new LayerWriteFeatureRequest();
            features.stream().forEach(feature -> {
                XyzFeature editedFeature=feature.encodeFeature(true).getFeature();
                editedFeature.setGeometry(new XyzPoint(1d, 1d));
                editedFeature.getProperties().put("testProperty","test");
                writeRequest.add(EWriteOp.PUT, editedFeature);
            });
            try (ForwardCursor<XyzFeature, XyzFeatureCodec> writeCursor =
                         writeSession.execute(writeRequest).getXyzFeatureCursor()) {
                assertTrue(writeCursor.hasNext());
                writeCursor.next();
                XyzFeature feature=writeCursor.getFeature();
                assertEquals(1d, feature.getGeometry().getJTSGeometry().getCoordinate().x);
                assertTrue(feature.getProperties().containsKey("testProperty"));
                assertEquals("test",feature.getProperties().get("testProperty").toString());
                assertSame(EExecutedOp.UPDATED, writeCursor.getOp());

                writeSession.commit(true);
            }

            //Check if the feature updated in expected storage collection
            ViewLayerCollection readViewCollection = new ViewLayerCollection("ReadLayer", layer0);
            view = new View(readViewCollection);

            List<XyzFeatureCodec> list=queryView(view,readRequest);
            assertTrue(list.size()==1);
            XyzFeature updatedFeature=list.get(0).encodeFeature(true).getFeature();
            assertEquals(1d, updatedFeature.getGeometry().getJTSGeometry().getCoordinate().x);
            assertTrue(updatedFeature.getProperties().containsKey("testProperty"));
            assertEquals("test",updatedFeature.getProperties().get("testProperty").toString());

            session.commit(true);
        }
    }
    @Test
    @Order(17)
    @EnabledIf("runTest")
    void featureMissingInCollection1() throws NoCursor {
        assertNotNull(storage);

        ViewLayer layer1 = new ViewLayer(storage, COLLECTION_1);

        ViewLayerCollection viewLayerCollection = new ViewLayerCollection("Layers", layer1);
        View view = new View(viewLayerCollection);

        ReadFeatures readRequest = new ReadFeatures();
        readRequest.setPropertyOp(POp.eq(PRef.id(),"feature_id_view0"));

        List<XyzFeatureCodec> list=queryView(view,readRequest);
        assertTrue(list.size()==0);
    }

    @Test
    @Order(18)
    @EnabledIf("runTest")
    void writeFeatureOnSelectedLayer() throws NoCursor {
        assertNotNull(storage);

        ViewLayer layer0 = new ViewLayer(storage, COLLECTION_0);
        ViewLayer layer1 = new ViewLayer(storage, COLLECTION_1);

        ViewLayerCollection viewLayerCollection = new ViewLayerCollection("Layers",layer0, layer1);
        View view = new View(viewLayerCollection);

        try (ViewWriteSession writeSession = view.newWriteSession(nakshaContext,true).withWriteLayer(layer1).init()) {
            LayerWriteFeatureRequest writeRequest=new LayerWriteFeatureRequest();
            final XyzFeature feature = fg.newRandomFeature();
            feature.setGeometry(new XyzPoint(0d, 0d));
            feature.setId("feature_id_view1");
            writeRequest.add(EWriteOp.PUT, feature);

            try (ForwardCursor<XyzFeature, XyzFeatureCodec> writeCursor =
                         writeSession.execute(writeRequest).getXyzFeatureCursor()) {
                assertTrue(writeCursor.hasNext());
                writeCursor.next();
                assertSame(EExecutedOp.CREATED, writeCursor.getOp());
            }
            writeSession.commit(true);

            //check if the newly added feature found on layer
            ReadFeatures readRequest = new ReadFeatures();
            readRequest.setPropertyOp(POp.eq(PRef.id(),"feature_id_view1"));

            List<XyzFeatureCodec> list=queryView(view,readRequest);
            assertTrue(list.size()==1);
        }
        session.commit(true);
    }

    private List<XyzFeatureCodec> queryView(View view, ReadFeatures request) throws NoCursor {
        ViewReadSession readSession = view.newReadSession(nakshaContext, false);
        try (final SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
                     readSession.execute(request).getXyzSeekableCursor()) {
            return cursor.asList();
        } finally {
            readSession.close();
        }
    }

}
