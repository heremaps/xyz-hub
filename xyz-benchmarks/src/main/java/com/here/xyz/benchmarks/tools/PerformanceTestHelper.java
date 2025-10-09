package com.here.xyz.benchmarks.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.Typed;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.models.geojson.WebMercatorTile;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.NLConnector;
import com.here.xyz.psql.PSQLXyzConnector;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;
import static com.here.xyz.events.ModifySpaceEvent.Operation.DELETE;
import static com.here.xyz.events.PropertyQuery.QueryOperation.EQUALS;
import static com.here.xyz.events.PropertyQuery.QueryOperation.BEGINS_WITH;
import static com.here.xyz.events.UpdateStrategy.OnExists.ERROR;

public class PerformanceTestHelper {
  public static final String DEFAULT_HOST = "localhost";
  private static final int AVG_PAYLOAD_BYTE_SIZE = 5289;
  public final static UpdateStrategy SEEDING_STRATEGY
          = new UpdateStrategy(ERROR, UpdateStrategy.OnNotExists.CREATE, null, null);

  private static final Logger logger = LogManager.getLogger();

  public static Typed createSpace(StorageConnector connector, String spaceName) throws Exception {
    return modifySpace(connector, spaceName, CREATE);
  }

  public static Typed deleteSpace(StorageConnector connector, String spaceName) throws Exception {
    return modifySpace(connector, spaceName, DELETE);
  }

  public static Typed writeFeatureCollectionIntoSpace(StorageConnector connector, String spaceName, UpdateStrategy updateStrategy,
                                                      FeatureCollection fc)
          throws Exception {

    WriteFeaturesEvent writeFeaturesEvent = new WriteFeaturesEvent()
            .withModifications(Set.of(
                    new Modification()
                            .withFeatureData(fc.copy())
                            .withUpdateStrategy(updateStrategy)
            )).withSpace(spaceName);

    Typed xyzResponse = connector.handleEvent(writeFeaturesEvent);
    logger.info("Response of WriteFeaturesEvent: {}", xyzResponse.serialize());

    return xyzResponse;
  }

  public static Typed deleteFeaturesFromSpace(StorageConnector connector, String spaceName, UpdateStrategy updateStrategy,
                                              List<String> featureIds)
          throws Exception {

    WriteFeaturesEvent writeFeaturesEvent = new WriteFeaturesEvent()
            .withModifications(Set.of(
                    new Modification()
                            .withFeatureIds(featureIds)
                            .withUpdateStrategy(new UpdateStrategy(UpdateStrategy.OnExists.DELETE, UpdateStrategy.OnNotExists.RETAIN, null, null))
            )).withSpace(spaceName);

    Typed xyzResponse = connector.handleEvent(writeFeaturesEvent);
    logger.info("Response of WriteFeaturesEvent: {}", xyzResponse.serialize());

    return xyzResponse;
  }

  public static Typed readFeaturesByRefQuad(StorageConnector connector, String spaceName, String refQuad, int limit)
          throws Exception {

    return readFeaturesByRefQuad(connector, spaceName, refQuad, limit, false);
  }

  public static Typed readFeaturesByRefQuad(StorageConnector connector, String spaceName, String refQuad, int limit, boolean isCount)
          throws Exception {
    return readFeaturesByRefQuadAndGlobalVersions(connector, spaceName, refQuad, null, limit, isCount);
  }

  public static Typed readFeaturesByGlobalVersions(StorageConnector connector, String spaceName, List<Integer> globalVersions, int limit, boolean isCount)
          throws Exception {
    return readFeaturesByRefQuadAndGlobalVersions(connector, spaceName, null, globalVersions, limit, isCount);
  }

  public static Typed readFeaturesByRefQuadAndGlobalVersions(StorageConnector connector, String spaceName, String refQuad,
                                                             List<Integer> globalVersions, int limit, boolean isCount)
          throws Exception {
    Event searchForFeaturesEvent;

    PropertiesQuery propertiesQuery = new PropertiesQuery();
    PropertyQueryList queries = new PropertyQueryList();

    if (refQuad != null)
      queries.add(new PropertyQuery()
              .withKey("properties.refQuad")
              .withOperation(BEGINS_WITH)
              .withValues(List.of(refQuad)));

    if (globalVersions != null)
      queries.add(new PropertyQuery()
              .withKey("properties.globalVersions")
              .withOperation(EQUALS)
              .withValues(Collections.singletonList(globalVersions)));

    propertiesQuery.add(queries);
    searchForFeaturesEvent = new SearchForFeaturesEvent()
            .withLimit(limit)
            .withPropertiesQuery(propertiesQuery)
            .withSpace(spaceName);

    if (isCount)
      ((SearchForFeaturesEvent) searchForFeaturesEvent).setSelection(List.of("f.refQuadCount"));

    Typed xyzResponse = connector.handleEvent(searchForFeaturesEvent);
    logger.info("Response of SearchForFeaturesEvent: {}", xyzResponse.serialize());

    return xyzResponse;
  }

  public static Typed readFeaturesTile(StorageConnector connector, String spaceName, String tid, int limit)
          throws Exception {

    Event getFeaturesByTileEvent = new GetFeaturesByTileEvent()
            .withLimit(limit)
            .withBbox(WebMercatorTile.forQuadkey(tid).getBBox(false))
            .withSpace(spaceName);

    Typed xyzResponse = connector.handleEvent(getFeaturesByTileEvent);
    logger.info("Response of readFeaturesTile: {}", xyzResponse.serialize());

    return xyzResponse;
  }

  public static Typed readFeaturesByBBox(StorageConnector connector, String spaceName, BBox bBox, int limit)
          throws Exception {

    Event getFeaturesByTileEvent = new GetFeaturesByTileEvent()
            .withVersionsToKeep(100_000)
            .withLimit(limit)
            .withBbox(bBox)
            .withSpace(spaceName);

    Typed xyzResponse = connector.handleEvent(getFeaturesByTileEvent);
    logger.info("Response of readFeaturesByBBox: {}", xyzResponse.serialize());

    return xyzResponse;
  }

  public static Typed readFeaturesByIds(StorageConnector connector, String spaceName, List<String> ids)
          throws Exception {

    Event getFeaturesByTileEvent = new GetFeaturesByIdEvent()
            .withIds(ids)
            .withSpace(spaceName);

    Typed xyzResponse = connector.handleEvent(getFeaturesByTileEvent);
    logger.info("Response of readFeaturesByIds: {}", xyzResponse.serialize());

    return xyzResponse;
  }

  public static Typed modifySpace(StorageConnector connector, String spaceName, Operation operation) throws Exception {

    ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent()
            .withSpaceDefinition(new Space()
                    .withId(spaceName)
                    .withVersionsToKeep(10_000_000)) //needed for PSQLConnector
            .withSpace(spaceName)
            .withOperation(operation);

    Typed xyzResponse = connector.handleEvent(modifySpaceEvent);
    logger.info("Response of ModifySpaceEvent[{}]: {}", operation.name(), xyzResponse.serialize());
    return xyzResponse;
  }

  public static DatabaseSettings createDBSettings(String host, String user, String db, String password, int maxPoolSize) {
    return new DatabaseSettings(host.equalsIgnoreCase(DEFAULT_HOST) ? "local_db" : "custom_db")
            .withDb(db)
            .withHost(host)
            .withPort(5432)
            .withUser(user)
            .withPassword(password)
            .withDbInitialPoolSize(10)
            .withDbMaxPoolSize(maxPoolSize)
            .withDbCheckoutTimeout(7000)
            .withDbAcquireRetryAttempts(5);
  }

  public static <T extends DatabaseHandler> T initConnector(String functionName, T connector, DatabaseSettings dbSettings) {
    return initConnector(functionName, connector, dbSettings, false);
  }

  public static <T extends DatabaseHandler> T initConnector(String functionName, T connector, DatabaseSettings dbSettings, boolean seedingMode) {
    SimulatedContext context = new SimulatedContext(functionName, null)
            .withIsRecreateLambdaEnvForEachEvent(true);
    connector.initialize(dbSettings, context);
    if (connector instanceof NLConnector nlConnector)
      nlConnector.setSeedingMode(seedingMode);
    return connector;
  }

  public static FeatureCollection generateRandomFeatureCollection(int featureCnt, float xmin, float ymin, float xmax, float ymax) throws JsonProcessingException {
    return generateRandomFeatureCollection(featureCnt, xmin, ymin, xmax, ymax, AVG_PAYLOAD_BYTE_SIZE, false);
  }

  public static FeatureCollection generateRandomFeatureCollection(int featureCnt, float xmin, float ymin, float xmax, float ymax, int payloadByteSize, boolean createIds) throws JsonProcessingException {
    return generateRandomFeatureCollection(featureCnt, xmin, ymin, xmax, ymax, payloadByteSize, 0, createIds);
  }

  public static FeatureCollection generateRandomFeatureCollection(int featureCnt, float xmin, float ymin, float xmax, float ymax, int payloadByteSize, int startId, boolean createIds)
          throws JsonProcessingException {

    FeatureCollection fc = new FeatureCollection();
    ThreadLocalRandom random = ThreadLocalRandom.current();

    for (int i = startId; i < featureCnt; i++) {
      double x = xmin + random.nextDouble() * (xmax - xmin);
      double y = ymin + random.nextDouble() * (ymax - ymin);

      // Generate random payload directly as a byte[] or hex string
      byte[] payloadBytes = new byte[payloadByteSize / 2]; //Each byte is represented by 2 hex characters (0x00 → "00", 0xFF → "ff").
      random.nextBytes(payloadBytes);

      String payload = HexFormat.of().formatHex(payloadBytes);

      Properties properties = new Properties()
              .with("payload", payload)
              .with("refQuad", WebMercatorTile.getTileFromLatLonLev(y, x, 20).asQuadkey())
              .with("version", 1)
              .with("globalVersion", 1);

      Feature feature = new Feature()
              .withId(createIds ? "id_" + i : null)
              .withProperties(properties)
              .withGeometry(new Point().withCoordinates(new PointCoordinates(x, y)));

      fc.getFeatures().add(feature);
    }

    return fc;
  }

  private static final Random RANDOM = new Random();

  public static String randomChildQuadkey(String parentQuadkey, int targetLevel) {
    if (parentQuadkey == null || parentQuadkey.isEmpty()) {
      throw new IllegalArgumentException("Parent quadkey must not be null or empty");
    }
    if (targetLevel < parentQuadkey.length()) {
      throw new IllegalArgumentException("Target level must be >= parent quadkey length");
    }

    StringBuilder sb = new StringBuilder(parentQuadkey);

    while (sb.length() < targetLevel) {
      int digit = RANDOM.nextInt(4); // 0,1,2,3
      sb.append(digit);
    }

    return sb.toString();
  }

  public static String getSpaceName(StorageConnector c, String name) {
    if (c instanceof NLConnector)
      return "new_layout_" + name;
    else if (c instanceof PSQLXyzConnector)
      return "old_layout_" + name;
    return name;
  }

  public static void main(String[] args) {
    System.out.println(randomChildQuadkey("1202032", 18));
    System.out.println(WebMercatorTile.forQuadkey("1202032").getBBox(false));
  }
}
