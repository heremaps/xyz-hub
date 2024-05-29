package com.here.naksha.app.data;

import static java.util.stream.Stream.generate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.app.common.NakshaTestWebClient;
import com.here.naksha.app.common.TestUtil;
import com.here.naksha.app.service.models.FeatureCollectionRequest;
import com.here.naksha.lib.core.models.geojson.WebMercatorTile;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.geojson.coordinates.LineStringCoordinates;
import com.here.naksha.lib.core.models.geojson.coordinates.PointCoordinates;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzLineString;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GenerativeDataIngest extends AbstractDataIngest {

  private static final Logger logger = LoggerFactory.getLogger(GenerativeDataIngest.class);

  private static final int FEATURES_IN_BATCH_LIMIT = 200;

  private static final int FEATURES_PER_TILE = 10;

  private static final String TILE_IDS_FILE = "topology/tile_ids.csv";

  private static final boolean GENERATED_FEATURES_INGEST_ENABLED = false;

  private final List<String> tileIds;

  private final Random random;

  private final TopologyFeatureGenerator topologyFeatureGenerator;

  public GenerativeDataIngest() {
    this.tileIds = tileIds();
    this.random = ThreadLocalRandom.current();
    this.topologyFeatureGenerator = new TopologyFeatureGenerator(random);
  }

  @Test
  @EnabledIf("isGeneratedFeaturesIngestEnabled")
  void ingestRandomFeatures() throws URISyntaxException, IOException, InterruptedException {
    setNHUrl(nhUrl);
    setNHToken(nhToken);
    setNHSpaceId("ingest_test_space");
    setNakshaClient(new NakshaTestWebClient(nhUrl, 10, 90));

    logger.info("Ingesting {} tiles with {} generated Topology features each, using NH Url [{}], in Space [{}]", tileIds.size(), FEATURES_PER_TILE, nhUrl, nhSpaceId);

    ingestRandomFeatures(FEATURES_PER_TILE);
  }

  private void ingestRandomFeatures(int featuresPerTile) throws URISyntaxException, IOException, InterruptedException {
    String streamId = UUID.randomUUID().toString();
    int maxTilesInBatch = FEATURES_IN_BATCH_LIMIT / featuresPerTile;
    int ingestedTiles = 0;
    while (ingestedTiles < tileIds.size()){
      int currentBatchTilesCount = Math.min(maxTilesInBatch, tileIds.size() - ingestedTiles);
      List<String> tilesInBatch = tileIds.subList(ingestedTiles, ingestedTiles + currentBatchTilesCount);
      String batchRequest = batchRequest(tilesInBatch, featuresPerTile);
      logger.info("Populating {} tiles: {}, {} features per tile", tilesInBatch.size(), String.join(",", tilesInBatch), featuresPerTile);
      sendFeaturesToNaksha(batchRequest, streamId);
      ingestedTiles += tilesInBatch.size();
      logger.info("Ingested features for {} tiles, {} tiles left", ingestedTiles, tileIds.size() - ingestedTiles);
    }
  }

  private void sendFeaturesToNaksha(String requestBody, String streamId) throws URISyntaxException, IOException, InterruptedException {
    HttpResponse<String> response = nakshaClient.put(
        "hub/spaces/" + nhSpaceId + "/features?access_token=" + nhToken, requestBody, streamId);
    assertEquals(
        200,
        response.statusCode(),
        "ResCode mismatch while importing batch with streamId" + streamId);
  }

  private String batchRequest(List<String> tileIds, int featuresPerTile){
    List<XyzFeature> featuresInBatch = tileIds.stream()
        .map(tileId -> featuresForTile(tileId, featuresPerTile))
        .flatMap(List::stream)
        .toList();
    return new FeatureCollectionRequest()
        .withFeatures(featuresInBatch)
        .serialize();
  }

  private List<XyzFeature> featuresForTile(String tileId, int count){
    return generate(() -> topologyFeatureGenerator.randomFeatureForTile(tileId))
        .limit(count)
        .toList();
  }
  private boolean isGeneratedFeaturesIngestEnabled() {
    return GENERATED_FEATURES_INGEST_ENABLED;
  }

  private static List<String> tileIds() {
    String[] rawCsv = TestUtil.loadFileOrFail(DATA_ROOT_FOLDER, TILE_IDS_FILE).split("\n");
    return Arrays.stream(rawCsv)
        .skip(1)
        .toList();
  }

  static class TopologyFeatureGenerator {

    private static final String ID_PREFIX = "generated_feature_";
    private static final String SAMPLES_DIR = "src/test/resources/ingest_data/";
    private static final String BASE_JSON = "topology/sample_topology_feature.json";

    private static final AtomicLong COUNTER = new AtomicLong(0);
    private final Random random;
    private final XyzFeature baseFeature;

    public TopologyFeatureGenerator(Random random) {
      this.random = random;
      this.baseFeature = TestUtil.parseJsonFileOrFail(SAMPLES_DIR, BASE_JSON, XyzFeature.class);
    }

    XyzFeature randomFeatureForTile(String tileId) {
      XyzFeature generated = new XyzFeature(ID_PREFIX + COUNTER.incrementAndGet());
      generated.setProperties(baseFeature.getProperties());
      generated.setGeometry(randomLineInTile(tileId));
      generated.setBbox(null);
      return generated;
    }

    private XyzLineString randomLineInTile(String tileId) {
      BBox tileBbox = WebMercatorTile.forQuadkey(tileId).getBBox(false);
      double lonDist = tileBbox.maxLon() - tileBbox.minLon();
      double latDist = tileBbox.maxLat() - tileBbox.minLat();
      double currentLon = tileBbox.minLon() + random.nextDouble(lonDist);
      double currentLat = tileBbox.minLat() + random.nextDouble(latDist);
      int pointsInLine = random.nextInt(2, 10);
      LineStringCoordinates coordinates = new LineStringCoordinates();
      for (int i = 0; i < pointsInLine; i++) {
        coordinates.add(new PointCoordinates(currentLon, currentLat));
        currentLon = currentLon + random.nextDouble(tileBbox.maxLon() - currentLon);
        currentLat = currentLat + random.nextDouble(tileBbox.maxLat() - currentLat);
      }
      return new XyzLineString().withCoordinates(coordinates);
    }
  }
}
