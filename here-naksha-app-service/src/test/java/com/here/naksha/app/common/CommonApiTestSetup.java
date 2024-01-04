package com.here.naksha.app.common;

import static com.here.naksha.app.common.assertions.ResponseAssertions.assertThat;
import static com.here.naksha.app.common.TestUtil.loadFileOrFail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.util.UUID;

public class CommonApiTestSetup {

  private static final String CREATE_STORAGE_JSON = "create_storage.json";
  private static final String CREATE_HANDLER_JSON = "create_event_handler.json";
  private static final String CREATE_SPACE_JSON = "create_space.json";

  private CommonApiTestSetup() {
  }

  /**
   * Convenience method that creates resources needed for feature-related operations (Storage, Handler, Space) Client needs to supply
   * directory which contains corresponding json file for each of the resources (`create_storage.json`,`create_event_handler.json`,
   * `create_space.json`)
   *
   * @param nakshaClient Naksha http client used for creating resource via REST API
   * @param setupDir     subdirectory of 'src/test/resources/unit_test_data/' that contains resource definition in json format
   */
  public static void setupSpaceAndRelatedResources(NakshaTestWebClient nakshaClient, String setupDir) {
    try {
      createStorage(nakshaClient, setupDir + "/" + CREATE_STORAGE_JSON);
      createHandler(nakshaClient, setupDir + "/" + CREATE_HANDLER_JSON);
      createSpace(nakshaClient, setupDir + "/" + CREATE_SPACE_JSON);
    } catch (URISyntaxException | IOException | InterruptedException e) {
      throw new RuntimeException("Unable to run setup for dir: " + setupDir, e);
    }
  }

  public static void createSpace(NakshaTestWebClient nakshaClient, String spaceJsonFilePath)
      throws URISyntaxException, IOException, InterruptedException {
    createAdminEntity(nakshaClient, "hub/spaces", spaceJsonFilePath);
  }

  public static void createStorage(NakshaTestWebClient nakshaClient, String storageJsonFilePath)
      throws URISyntaxException, IOException, InterruptedException {
    createAdminEntity(nakshaClient, "hub/storages", storageJsonFilePath);
  }

  public static void createHandler(NakshaTestWebClient nakshaClient, String handlerJsonFilePath)
      throws URISyntaxException, IOException, InterruptedException {
    createAdminEntity(nakshaClient, "hub/handlers", handlerJsonFilePath);
  }

  private static void createAdminEntity(NakshaTestWebClient nakshaClient, String nakshaResourcePath, String jsonFilePath)
      throws URISyntaxException, IOException, InterruptedException {
    HttpResponse<String> response = nakshaClient.post(
        nakshaResourcePath,
        loadFileOrFail(jsonFilePath),
        UUID.randomUUID().toString()
    );
    assertThat(response).hasStatus(200);
  }

}
