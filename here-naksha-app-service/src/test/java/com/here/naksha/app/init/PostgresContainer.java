package com.here.naksha.app.init;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.InternetProtocol.TCP;
import static org.testcontainers.containers.wait.strategy.WaitAllStrategy.Mode.WITH_MAXIMUM_OUTER_TIMEOUT;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.psql.PsqlStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

public class PostgresContainer {

  private static final Logger log = LoggerFactory.getLogger(PostgresContainer.class);

  private static final Integer POSTGRES_CONTAINER_PORT = 5432;

  private static final Integer LOCALHOST_PORT = 5432;

  private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(90);

  private static final String DB_READY_LOG_PATTERN = ".*database system is ready to accept connections.*\\s";

  private final GenericContainer nakshaPostgres;

  private PostgresContainer() {
    nakshaPostgres = new GenericContainer(Config.imageIdFromConfig())
        .withEnv(Map.of(
            "POSTGRES_PASSWORD", "postgres",
            "POSTGRES_INITDB_ARGS", "--auth-host=trust --auth-local=trust",
            "POSTGRES_HOST_AUTH_METHOD", "trust"
        ));
    nakshaPostgres.setPortBindings(List.of("%s:%s/%s".formatted(LOCALHOST_PORT, POSTGRES_CONTAINER_PORT, TCP.toDockerNotation())));
    nakshaPostgres.setWaitStrategy(
        new WaitAllStrategy(WITH_MAXIMUM_OUTER_TIMEOUT)
            .withStartupTimeout(STARTUP_TIMEOUT)
            .withStrategy(Wait.forLogMessage(DB_READY_LOG_PATTERN, 2))
            .withStrategy(Wait.forListeningPort()));
  }

  public String getJdbcUrl() {
    return "jdbc:postgresql://localhost:" + LOCALHOST_PORT + "/postgres?user=postgres&password=password"
        + "&schema=" + TestPsqlStorageConfigs.dataDbConfig.schema()
        + "&app=" + "Naksha/v" + NakshaVersion.latest
        + "&id=" + PsqlStorage.ADMIN_STORAGE_ID;
  }

  public static PostgresContainer startedPostgresContainer() {
    PostgresContainer container = new PostgresContainer();
    container.nakshaPostgres.start();
    container.nakshaPostgres.followOutput(new Slf4jLogConsumer(log));
    return container;
  }

  public void stop() {
    log.info("Stopping Container...");
    nakshaPostgres.stop();
  }

  private static class Config {

    private static final String IMAGE_CONFIG_PATH = "src/test/psql_container/naksha_psql_image.conf";
    private static final String CONTAINER_REPOSITORY_KEY = "CONTAINER_REPOSITORY";
    private static final String IMAGE_NAMESPACE_KEY = "IMAGE_NAMESPACE";
    private static final String IMAGE_NAME_KEY = "IMAGE_NAME";
    private static final String IMAGE_VERSION_KEY = "IMAGE_VERSION";

    private Config() {
    }

    static @NotNull DockerImageName imageIdFromConfig() {
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
