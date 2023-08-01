/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.hub.util;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.restassured.RestAssured.given;

import com.here.xyz.hub.rest.TestSpaceWithFeature;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

public class PerformanceTest extends TestSpaceWithFeature {

  private static final int WRITE_LIMIT_OPERATIONS = Integer.MAX_VALUE;
  private static final int READ_LIMIT_OPERATIONS = Integer.MAX_VALUE;
  private static final int DELETE_LIMIT_OPERATIONS = Integer.MAX_VALUE;
  private static final int NUMBER_OF_FEATURES = 20_000;
  private static final int NUMBER_OF_TILE_REQUESTS = 2_000;
  private static final int NUMBER_OF_ROUNDS = 20;
  private static final StringBuffer report = new StringBuffer();

  private static long writeTotalMilliseconds;
  private static long readTotalMilliseconds;
  private static long quadbinTotalMilliseconds;
  private static long hexbinTotalMilliseconds;
  private static long vizmodeTotalMilliseconds;
  private static long deleteTotalMilliseconds;

  public static void main(String... args) {
    if (args.length != 2 ||
        !Arrays.asList("generate", "load").contains(args[0]) ||
        StringUtils.isBlank(args[1])) {
      System.err.println("Usage: java -cp <classpath> com.here.xyz.hub.util.PerformanceTest <generate|load> <filepath>");
      System.exit(1);
    }

    configureRestAssured();
    RestAssured.config = RestAssured.config().httpClient(RestAssured.config().getHttpClientConfig().dontReuseHttpClientInstance());

    String command = args[0];
    String filepath = args[1];

    System.out.println("generate...");
    generate(command, filepath);

    try {
      for (int i=0; i<NUMBER_OF_ROUNDS; i++) {
        report.append("---------------------------- BEGIN of ROUND ")
            .append(i+1)
            .append(" ----------------------------")
            .append(System.lineSeparator());

        System.out.println("Round " + i);
        System.out.println("prepareSpace...");
        prepareSpace();
        System.out.println("testWrite...");
        testWrite(filepath);
        System.out.println("testRead...");
        testRead(filepath);
        System.out.println("testClustering 1...");
        testClusteringQuadbin(filepath);
        System.out.println("testClustering 2...");
        testClusteringHexbin(filepath);
        System.out.println("testVizMode...");
        testVizMode();
        System.out.println("testDelete...");
        testDelete(filepath);

        report.append("---------------------------- END of ROUND ")
            .append(i+1)
            .append(" ----------------------------")
            .append(System.lineSeparator())
            .append(System.lineSeparator());
      }
      System.out.println("end");
      report(filepath);
    } finally {
      deleteSpace();
    }
  }

  private static void generate(String command, String filepath) {
    if (Command.get(command) == Command.GENERATE) {
      if (!Paths.get(filepath).toFile().exists()) {
        Paths.get(filepath).toFile().mkdirs();
      }

      if (!Paths.get(filepath).toFile().isDirectory()) {
        System.out.println("when using generate, filepath must be a directory.");
        System.exit(1);
      }

      System.out.println("cleanDirectory...");
      cleanDirectory(filepath);
      System.out.println("generateFeatures...");
      generateFeatures(filepath);
      System.out.println("generateTiles...");
      generateTiles(filepath);
    }
  }
  private static void prepareSpace() {
    remove();
    createSpace();
    given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces/x-psql-test/statistics")
        .then()
        .statusCode(OK.code());
  }

  private static void testWrite(String filepath) {
    report.append("---------------------------- BEGIN of WRITE tests ----------------------------").append(System.lineSeparator());

    final Hashtable<String, AtomicInteger> statusCodesCount = new Hashtable<>();
    final StringBuffer localErrors = new StringBuffer();
    final AtomicInteger numberOfErrors = new AtomicInteger();
    final AtomicInteger counter = new AtomicInteger();

    final ExecutorService ex = Executors.newFixedThreadPool(2);
    long begin = System.currentTimeMillis();
    try {
      Files
        .list(Paths.get(filepath))
        .map(Path::toFile)
        .filter(f -> f.getName().startsWith("fc_"))
        .filter(f -> f.getName().endsWith(".geojson"))
        .forEach(f -> {
          try {
            ex.execute(() -> {
              if (counter.get() > WRITE_LIMIT_OPERATIONS) return;

              ExtractableResponse<Response> r = given()
                  .contentType(APPLICATION_GEO_JSON)
                  .accept("application/x-empty")
                  .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
                  .body(load(f.getAbsolutePath()))
                  .when()
                  .post("/spaces/x-psql-test/features?transactional=false")
                  .then()
                  .extract();

              String statusCode = String.valueOf(r.statusCode());
              statusCodesCount.putIfAbsent(statusCode, new AtomicInteger());
              statusCodesCount.get(statusCode).incrementAndGet();

              if (r.statusCode() >= 400 && numberOfErrors.incrementAndGet() <= 10) {
                String errorBody = r.body().asString() + System.lineSeparator();
                localErrors.append(errorBody);
              }

              System.out.print("\r" + counter.incrementAndGet());
            });
          } catch(Exception ignored) {}
        });

      ex.shutdown();
      if (!ex.awaitTermination(60, TimeUnit.MINUTES)) {
        System.err.println("\rWrite test execution timed out after " + (System.currentTimeMillis() - begin) + " milliseconds.");
        System.exit(1);
      }

      long time = System.currentTimeMillis() - begin;
      writeTotalMilliseconds += time;
      report.append("Write test report for ")
          .append(counter.get())
          .append(" requests after ")
          .append(time)
          .append(" milliseconds.")
          .append(System.lineSeparator());

      report.append("Status\t\tCount").append(System.lineSeparator());
      for (Entry<String, AtomicInteger> e : statusCodesCount.entrySet()) {
        report.append(e.getKey()).append("\t\t\t\t").append(e.getValue().get()).append(System.lineSeparator());
      }

      if (localErrors.length() > 0) {
        report.append("Error responses (max 10 errors):").append(System.lineSeparator());
        report.append(localErrors).append(System.lineSeparator());
      }

      report.append("Number of features stored (estimated): ").append(getStatisticsCount()).append(System.lineSeparator());
    } catch (InterruptedException e) {
      System.err.println("\rWrite test execution interrupted after " + (System.currentTimeMillis() - begin) + " milliseconds.");
      System.exit(1);
    } catch(IOException e) {
      System.err.println("\rUnable to load feature collection from the filepath: " + filepath);
      System.exit(1);
    }

    System.out.println("");
  }

  private static void testClusteringQuadbin(String filepath) {
    report.append("---------------------------- BEGIN of QUADBIN tests ----------------------------").append(System.lineSeparator());

    final ExecutorService ex = Executors.newFixedThreadPool(4);
    long begin = System.currentTimeMillis();
    try {
      final Hashtable<String, AtomicInteger> statusCodesCount = new Hashtable<>();
      final StringBuffer localErrors = new StringBuffer();
      final AtomicInteger numberOfErrors = new AtomicInteger();
      final AtomicInteger counter = new AtomicInteger();

      Set<String> tiles = Files.readAllLines(Paths.get(filepath, "tiles.txt")).stream().map(t->t.substring(3)).collect(Collectors.toSet());
      for (String tileId : tiles) {
        ex.execute(() -> {
          if (counter.get() > READ_LIMIT_OPERATIONS) return;

          ExtractableResponse<Response> r = given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .when()
              .get("/spaces/x-psql-test/tile/quadkey/" + tileId + "?skipCache=true&clustering=quadbin&clustering.relativeResolution=3&clustering.countmode=mixed&margin=0&clip=true")
              .then()
              .extract();

          String statusCode = String.valueOf(r.statusCode());
          statusCodesCount.putIfAbsent(statusCode, new AtomicInteger());
          statusCodesCount.get(statusCode).incrementAndGet();

          if (r.statusCode() > 200 && numberOfErrors.incrementAndGet() <= 10) {
            String errorBody = r.body().asString() + System.lineSeparator();
            localErrors.append(errorBody);
          }

          System.out.print("\r" + counter.incrementAndGet());
        });
      }

      ex.shutdown();
      if (!ex.awaitTermination(60, TimeUnit.MINUTES)) {
        System.err.println("\rQuadbin test execution timed out after " + (System.currentTimeMillis() - begin) + " milliseconds.");
        System.exit(1);
      }

      long time = System.currentTimeMillis() - begin;
      quadbinTotalMilliseconds += time;
      report.append("Quadbin test report for ")
          .append(counter.get())
          .append(" requests after ")
          .append(time)
          .append(" milliseconds.")
          .append(System.lineSeparator())
          .append(System.lineSeparator());

      report.append("Status\t\tCount").append(System.lineSeparator());
      for (Entry<String, AtomicInteger> e : statusCodesCount.entrySet()) {
        report.append(e.getKey()).append("\t\t\t\t").append(e.getValue().get()).append(System.lineSeparator());
      }

      if (localErrors.length() > 0) {
        report.append("Error responses (max 10 errors):").append(System.lineSeparator());
        report.append(localErrors).append(System.lineSeparator());
      }
    } catch (InterruptedException e) {
      System.err.println("\rQuadbin test execution interrupted after " + (System.currentTimeMillis() - begin) + " milliseconds.");
      System.exit(1);
    } catch(IOException e) {
      System.err.println("\rUnable to load tiles from the filepath: " + filepath);
      System.exit(1);
    }

    System.out.println("");
  }

  private static void testClusteringHexbin(String filepath) {
    report.append("---------------------------- BEGIN of HEXBIN tests ----------------------------").append(System.lineSeparator());

    final ExecutorService ex = Executors.newFixedThreadPool(1);
    long begin = System.currentTimeMillis();
    try {
      final Hashtable<String, AtomicInteger> statusCodesCount = new Hashtable<>();
      final StringBuffer localErrors = new StringBuffer();
      final AtomicInteger numberOfErrors = new AtomicInteger();
      final AtomicInteger counter = new AtomicInteger();

      Set<String> tiles = Files.readAllLines(Paths.get(filepath, "tiles.txt")).stream().limit(10).map(t->t.substring(3)).collect(Collectors.toSet());
      for (String tileId : tiles) {
        ex.execute(() -> {
          if (counter.get() > READ_LIMIT_OPERATIONS) return;

          ExtractableResponse<Response> r = given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .when()
              .get("/spaces/x-psql-test/tile/quadkey/" + tileId + "?skipCache=true&clustering=hexbin&clustering.relativeResolution=0&clustering.countmode=mixed&margin=0&clip=true")
              .then()
              .extract();

          String statusCode = String.valueOf(r.statusCode());
          statusCodesCount.putIfAbsent(statusCode, new AtomicInteger());
          statusCodesCount.get(statusCode).incrementAndGet();

          if (r.statusCode() > 200 && numberOfErrors.incrementAndGet() <= 10) {
            String errorBody = r.body().asString() + System.lineSeparator();
            localErrors.append(errorBody);
          }

          System.out.print("\r" + counter.incrementAndGet());
        });
      }

      ex.shutdown();
      if (!ex.awaitTermination(60, TimeUnit.MINUTES)) {
        System.err.println("\rHexbin test execution timed out after " + (System.currentTimeMillis() - begin) + " milliseconds.");
        System.exit(1);
      }

      long time = System.currentTimeMillis() - begin;
      hexbinTotalMilliseconds += time;
      report.append("Hexbin test report for ")
          .append(counter.get())
          .append(" requests after ")
          .append(time)
          .append(" milliseconds.")
          .append(System.lineSeparator())
          .append(System.lineSeparator());

      report.append("Status\t\tCount").append(System.lineSeparator());
      for (Entry<String, AtomicInteger> e : statusCodesCount.entrySet()) {
        report.append(e.getKey()).append("\t\t\t\t").append(e.getValue().get()).append(System.lineSeparator());
      }

      if (localErrors.length() > 0) {
        report.append("Error responses (max 10 errors):").append(System.lineSeparator());
        report.append(localErrors).append(System.lineSeparator());
      }
    } catch (InterruptedException e) {
      System.err.println("\rHexbin test execution interrupted after " + (System.currentTimeMillis() - begin) + " milliseconds.");
      System.exit(1);
    } catch(IOException e) {
      System.err.println("\rUnable to load tiles from the filepath: " + filepath);
      System.exit(1);
    }

    System.out.println("");
  }

  private static void testVizMode() {
    report.append("---------------------------- BEGIN of VIZMODE tests ----------------------------").append(System.lineSeparator());

    final ExecutorService ex = Executors.newFixedThreadPool(4);
    long begin = System.currentTimeMillis();
    try {
      final Hashtable<String, AtomicInteger> statusCodesCount = new Hashtable<>();
      final StringBuffer localErrors = new StringBuffer();
      final AtomicInteger numberOfErrors = new AtomicInteger();
      final AtomicInteger counter = new AtomicInteger();

      List<String> tiles = generateTiles(3);
      for (String tileId : tiles) {
        ex.execute(() -> {
          if (counter.get() > READ_LIMIT_OPERATIONS) return;

          ExtractableResponse<Response> r = given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .when()
              .get("/spaces/x-psql-test/tile/quadkey/" + tileId + "?skipCache=true&mode=viz&vizSampling=high&margin=0&clip=true")
              .then()
              .extract();

          String statusCode = String.valueOf(r.statusCode());
          statusCodesCount.putIfAbsent(statusCode, new AtomicInteger());
          statusCodesCount.get(statusCode).incrementAndGet();

          if (r.statusCode() > 200 && numberOfErrors.incrementAndGet() <= 10) {
            String errorBody = r.body().asString() + System.lineSeparator();
            localErrors.append(errorBody);
          }

          System.out.print("\r" + counter.incrementAndGet());
        });
      }

      ex.shutdown();
      if (!ex.awaitTermination(60, TimeUnit.MINUTES)) {
        System.err.println("\rHexbin test execution timed out after " + (System.currentTimeMillis() - begin) + " milliseconds.");
        System.exit(1);
      }

      long time = System.currentTimeMillis() - begin;
      vizmodeTotalMilliseconds += time;
      report.append("Vizmode test report for ")
          .append(counter.get())
          .append(" requests after ")
          .append(time)
          .append(" milliseconds.")
          .append(System.lineSeparator())
          .append(System.lineSeparator());

      report.append("Status\t\tCount").append(System.lineSeparator());
      for (Entry<String, AtomicInteger> e : statusCodesCount.entrySet()) {
        report.append(e.getKey()).append("\t\t\t\t").append(e.getValue().get()).append(System.lineSeparator());
      }

      if (localErrors.length() > 0) {
        report.append("Error responses (max 10 errors):").append(System.lineSeparator());
        report.append(localErrors).append(System.lineSeparator());
      }
    } catch (InterruptedException e) {
      System.err.println("\rVizmode test execution interrupted after " + (System.currentTimeMillis() - begin) + " milliseconds.");
      System.exit(1);
    }

    System.out.println("");
  }

  private static int getStatisticsCount() {
    return given()
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .when()
        .get("/spaces/x-psql-test/statistics")
        .then()
        .extract()
        .path("count.value");
  }

  private static void testRead(String filepath) {
    report.append("---------------------------- BEGIN of READ tests ----------------------------").append(System.lineSeparator());

    final ExecutorService ex = Executors.newFixedThreadPool(4);
    long begin = System.currentTimeMillis();
    try {
      final Hashtable<String, AtomicInteger> statusCodesCount = new Hashtable<>();
      final StringBuffer localErrors = new StringBuffer();
      final AtomicInteger numberOfErrors = new AtomicInteger();
      final AtomicInteger counter = new AtomicInteger();

      List<String> tiles = Files.readAllLines(Paths.get(filepath, "tiles.txt"));
      for (String tileId : tiles) {
        ex.execute(() -> {
          if (counter.get() > READ_LIMIT_OPERATIONS) return;

          ExtractableResponse<Response> r = given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .when()
              .get("/spaces/x-psql-test/tile/quadkey/" + tileId + "?skipCache=true&limit=1000")
              .then()
              .extract();

          String statusCode = String.valueOf(r.statusCode());
          statusCodesCount.putIfAbsent(statusCode, new AtomicInteger());
          statusCodesCount.get(statusCode).incrementAndGet();

          if (r.statusCode() > 200 && numberOfErrors.incrementAndGet() <= 10) {
            String errorBody = r.body().asString() + System.lineSeparator();
            localErrors.append(errorBody);
          }

          System.out.print("\r" + counter.incrementAndGet());
        });
      }

      ex.shutdown();
      if (!ex.awaitTermination(60, TimeUnit.MINUTES)) {
        System.err.println("\rRead test execution timed out after " + (System.currentTimeMillis() - begin) + " milliseconds.");
        System.exit(1);
      }

      long time = System.currentTimeMillis() - begin;
      readTotalMilliseconds += time;
      report.append("Read test report for ")
          .append(counter.get())
          .append(" requests after ")
          .append(time)
          .append(" milliseconds.")
          .append(System.lineSeparator())
          .append(System.lineSeparator());

      report.append("Status\t\tCount").append(System.lineSeparator());
      for (Entry<String, AtomicInteger> e : statusCodesCount.entrySet()) {
        report.append(e.getKey()).append("\t\t\t\t").append(e.getValue().get()).append(System.lineSeparator());
      }

      if (localErrors.length() > 0) {
        report.append("Error responses (max 10 errors):").append(System.lineSeparator());
        report.append(localErrors).append(System.lineSeparator());
      }
    } catch (InterruptedException e) {
      System.err.println("\rRead test execution interrupted after " + (System.currentTimeMillis() - begin) + " milliseconds.");
      System.exit(1);
    } catch(IOException e) {
      System.err.println("\rUnable to load tiles from the filepath: " + filepath);
      System.exit(1);
    }

    System.out.println("");
  }

  private static void testDelete(String filepath) {
    report.append("---------------------------- BEGIN of DELETE tests ----------------------------").append(System.lineSeparator());

    final ExecutorService ex = Executors.newFixedThreadPool(4);
    long begin = System.currentTimeMillis();
    try {
      final Hashtable<String, AtomicInteger> statusCodesCount = new Hashtable<>();
      final StringBuffer localErrors = new StringBuffer();
      final AtomicInteger numberOfErrors = new AtomicInteger();
      final AtomicInteger counter = new AtomicInteger();

      List<String> idsFromFile = Files.readAllLines(Paths.get(filepath, "ids.txt"));
      for (int i=0; i<idsFromFile.size();) {
        int end = Math.min(i + 10, idsFromFile.size());
        String ids = String.join(",", idsFromFile.subList(i, end));
        ex.execute(() -> {
          if (counter.get() > DELETE_LIMIT_OPERATIONS) return;

          ExtractableResponse<Response> r = given()
              .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
              .when()
              .delete("/spaces/x-psql-test/features?id=" + ids)
              .then()
              .extract();

          String statusCode = String.valueOf(r.statusCode());
          statusCodesCount.putIfAbsent(statusCode, new AtomicInteger());
          statusCodesCount.get(statusCode).incrementAndGet();

          if (r.statusCode() >= 400 && numberOfErrors.incrementAndGet() <= 10) {
            String errorBody = r.body().asString() + System.lineSeparator();
            localErrors.append(errorBody);
          }

          System.out.print("\r" + counter.incrementAndGet());
        });
        i=i+10;
      }

      ex.shutdown();
      if (!ex.awaitTermination(60, TimeUnit.MINUTES)) {
        System.err.println("\rDelete test execution timed out after " + (System.currentTimeMillis() - begin) + " milliseconds.");
        System.exit(1);
      }

      long time = System.currentTimeMillis() - begin;
      deleteTotalMilliseconds += time;
      report.append("Delete test report for ")
          .append(counter.get())
          .append(" requests after ")
          .append(time)
          .append(" milliseconds.")
          .append(System.lineSeparator())
          .append(System.lineSeparator());

      report.append("Status\t\tCount").append(System.lineSeparator());
      for (Entry<String, AtomicInteger> e : statusCodesCount.entrySet()) {
        report.append(e.getKey()).append("\t\t\t\t").append(e.getValue().get()).append(System.lineSeparator());
      }

      if (localErrors.length() > 0) {
        report.append("Error responses (max 10 errors):").append(System.lineSeparator());
        report.append(localErrors).append(System.lineSeparator());
      }
    } catch (InterruptedException e) {
      System.err.println("\rDelete test execution interrupted after " + (System.currentTimeMillis() - begin) + " milliseconds.");
      System.exit(1);
    } catch(IOException e) {
      System.err.println("\rUnable to load tiles from the filepath: " + filepath);
      System.exit(1);
    }

    System.out.println("");
  }

  private static void report(String filepath) {
    report.append(System.lineSeparator());
    report.append("---------------------------- Summary after ")
        .append(NUMBER_OF_ROUNDS)
        .append(" rounds ----------------------------")
        .append(System.lineSeparator());

    report.append("Write total time: ").append(writeTotalMilliseconds).append(" milliseconds, average: ").append(writeTotalMilliseconds/NUMBER_OF_ROUNDS).append(" milliseconds").append(System.lineSeparator());
    report.append("Read total time: ").append(readTotalMilliseconds).append(" milliseconds, average: ").append(readTotalMilliseconds/NUMBER_OF_ROUNDS).append(" milliseconds").append(System.lineSeparator());
    report.append("Quadbin total time: ").append(quadbinTotalMilliseconds).append(" milliseconds, average: ").append(quadbinTotalMilliseconds/NUMBER_OF_ROUNDS).append(" milliseconds").append(System.lineSeparator());
    report.append("Hexbin total time: ").append(hexbinTotalMilliseconds).append(" milliseconds, average: ").append(hexbinTotalMilliseconds/NUMBER_OF_ROUNDS).append(" milliseconds").append(System.lineSeparator());
    report.append("VizMode total time: ").append(vizmodeTotalMilliseconds).append(" milliseconds, average: ").append(vizmodeTotalMilliseconds/NUMBER_OF_ROUNDS).append(" milliseconds").append(System.lineSeparator());
    report.append("Delete total time: ").append(deleteTotalMilliseconds).append(" milliseconds, average: ").append(deleteTotalMilliseconds/NUMBER_OF_ROUNDS).append(" milliseconds").append(System.lineSeparator());
    report.append("---------------------------- Summary end ----------------------------").append(System.lineSeparator());

    try {
      Files.write(Paths.get(filepath, "report-"+System.currentTimeMillis()+".txt"), report.toString().getBytes(StandardCharsets.UTF_8));
    } catch (Exception ignore) {}
    System.out.println(report);
  }

  private static void deleteSpace() {
    remove();
  }

  private static void cleanDirectory(String filepath) {
    try {
      Files.deleteIfExists(Paths.get(filepath, "tiles.txt"));
      Files.deleteIfExists(Paths.get(filepath, "ids.txt"));

      Files
          .list(Paths.get(filepath))
          .filter(p -> p.toFile().getName().startsWith("fc_"))
          .forEach(p -> {
            try {
              Files.deleteIfExists(p);
            } catch (IOException ignore) {}
          });
    } catch (Exception e) {
      System.out.println("Unable to clean directory " + filepath);
      System.exit(1);
    }
  }

  private static void generateFeatures(String filepath) {
    final ExecutorService ex = Executors.newFixedThreadPool(4);
    final List<String> ids = Collections.synchronizedList(new ArrayList<>());

    long begin = System.currentTimeMillis();
    try {
      int iterations = (int) Math.ceil(NUMBER_OF_FEATURES / 1_000d);
      for (int i=0; i<iterations; i++) {
        int ii = i;
        ex.execute(()->{
          FeatureCollection featureCollection = generateRandomFeatures(NUMBER_OF_FEATURES/iterations, 8, true);
          try {
            List<String> generatedIds = featureCollection.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
            ids.addAll(generatedIds);

            Files.write(Paths.get(filepath, "fc_"+ ii +".geojson"), featureCollection.serialize().getBytes(StandardCharsets.UTF_8));
          } catch (Exception ignored) {}
        });
      }

      ex.shutdown();
      if (!ex.awaitTermination(60, TimeUnit.MINUTES)) {
        System.err.println("Generate features execution timed out after " + (System.currentTimeMillis() - begin) + " milliseconds.");
        System.exit(1);
      }

      Files.write(Paths.get(filepath, "ids.txt"), String.join(System.lineSeparator(), ids).getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      System.err.println("Unable to write feature collections or ids file to the directory: " + filepath);
      System.exit(1);
    }
  }

  private static byte[] load(String filepath) {
    try {
      return Files.readAllBytes(Paths.get(filepath));
    } catch (Exception e) {
      System.err.println("Unable to load feature collection from the file: " + filepath);
      System.exit(1);
    }

    return null;
  }

  private static List<String> generateTiles(int tileIdSize) {
    final int constrainedTileIdSize = Math.min(3,Math.max(1, tileIdSize));
    return new ArrayList<String>() {{
      for (int size=1; size<=constrainedTileIdSize; size++) {
        int[] result = new int[size];
        int last = size-1;
        int pos = last-1;
        while (true) {
          if (result[last] <= 3) {
            add(IntStream.of(result).mapToObj(val -> Character.forDigit(val, 10)).map(String::valueOf).collect(Collectors.joining()));
            result[last]++;
            continue;
          }

          if (pos < 0) break;

          if (result[pos] < 3) {
            result[pos]++;
            result[last] = 0;
            pos = last-1;
            continue;
          }

          if (result[pos] == 3) {
            result[pos] = 0;
            pos--;
          }
        }
      }
    }};
  }

  private static List<String> generateTiles(int minSize, int maxSize, int quantity) {
    return new ArrayList<String>() {{
      for (int i=0;i<quantity; i++) {
        add(RandomStringUtils.random(RandomUtils.nextInt(minSize,maxSize+1), '0', '1', '2', '3'));
      }
    }};
  }

  private static void generateTiles(String filepath) {
    try {
      String tiles = generateTiles(5,7,NUMBER_OF_TILE_REQUESTS).stream().collect(Collectors.joining(System.lineSeparator()));
      Files.write(Paths.get(filepath, "tiles.txt"), tiles.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      System.out.println("Unable to store tiles file at " + filepath);
      System.exit(1);
    }
  }

  private enum Command {
    GENERATE,
    LOAD;

    public static Command get(String name) {
      return Enum.valueOf(Command.class, name.trim().toUpperCase());
    }
  }
}
