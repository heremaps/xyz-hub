package com.here.naksha.app.init;

import static java.util.Objects.requireNonNull;

import com.here.naksha.app.common.TestUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class PostgresContainer {

  private static final Integer PORT = 5432;
  private final GenericContainer nakshaPostgres;

  public PostgresContainer() {
    nakshaPostgres = new GenericContainer(Config.imageIdFromConfig()).withExposedPorts(PORT);
  }

  public void start(){
    nakshaPostgres.start();
  }

  public void stop(){
    nakshaPostgres.stop();
  }

  private static class Config {
    private static final String IMAGE_CONFIG_PATH = "psql_container/naksha_psql_image.conf";
    private static final String CONTAINER_REPOSITORY_KEY = "CONTAINER_REPOSITORY";
    private static final String IMAGE_NAMESPACE_KEY = "IMAGE_NAMESPACE";
    private static final String IMAGE_NAME_KEY = "IMAGE_NAME";
    private static final String IMAGE_VERSION_KEY = "IMAGE_VERSION";

    private Config(){}

    static @NotNull DockerImageName imageIdFromConfig(){
      Properties properties = new Properties();
      try {
        properties.load(Files.newBufferedReader(Paths.get(IMAGE_CONFIG_PATH)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      String repo = requireNonNull(properties.getProperty(CONTAINER_REPOSITORY_KEY));
      String namespace = requireNonNull(properties.getProperty(IMAGE_NAMESPACE_KEY));
      String name = requireNonNull(properties.getProperty(IMAGE_NAME_KEY));
      String version = requireNonNull(properties.getProperty(IMAGE_VERSION_KEY));
      return DockerImageName.parse(repo + "/" + namespace + "/" + name + ":" + version);
    }
  }
}
