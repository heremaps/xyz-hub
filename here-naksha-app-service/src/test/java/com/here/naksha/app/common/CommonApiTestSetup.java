package com.here.naksha.app.common;

import static com.here.naksha.app.common.ResponseAssertions.assertThat;
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

  public static void setupSpaceAndRelatedResources(NakshaTestWebClient nakshaClient, String setupDir) {
    try {
      createStorage(nakshaClient, setupDir);
      createHandler(nakshaClient, setupDir);
      createSpace(nakshaClient, setupDir);
    } catch (URISyntaxException | IOException | InterruptedException e) {
      throw new RuntimeException("Unable to run setup for dir: " + setupDir, e);
    }
  }

  private static void createSpace(NakshaTestWebClient nakshaClient, String setupDirPath)
      throws URISyntaxException, IOException, InterruptedException {
    createAdminEntity(nakshaClient, "hub/spaces", setupDirPath + "/" + CREATE_SPACE_JSON);
  }

  private static void createStorage(NakshaTestWebClient nakshaClient, String setupDirPath)
      throws URISyntaxException, IOException, InterruptedException {
    createAdminEntity(nakshaClient, "hub/storages", setupDirPath + "/" + CREATE_STORAGE_JSON);
  }

  private static void createHandler(NakshaTestWebClient nakshaClient, String setupDirPath)
      throws URISyntaxException, IOException, InterruptedException {
    createAdminEntity(nakshaClient, "hub/handlers", setupDirPath + "/" + CREATE_HANDLER_JSON);
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
