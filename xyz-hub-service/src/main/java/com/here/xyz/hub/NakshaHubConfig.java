package com.here.xyz.hub;

import static com.here.xyz.NakshaLogger.currentLogger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.mapcreator.ext.naksha.PsqlConfig;
import com.here.mapcreator.ext.naksha.PsqlConfigBuilder;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The XYZ-Hub service configuration.
 */
@SuppressWarnings("UnusedReturnValue")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class NakshaHubConfig {

  /**
   * The application name for the configuration directory ({@code ~/.config/<appname>/...}).
   */
  public static final String APP_NAME = "naksha";

  @JsonCreator
  NakshaHubConfig(
      @JsonProperty @Nullable String serverName,
      @JsonProperty @Nullable String hostId,
      @JsonProperty @Nullable PsqlConfig db,
      @JsonProperty @Nullable Integer httpPort,
      @JsonProperty @Nullable String hostname,
      @JsonProperty @Nullable String endpoint,
      @JsonProperty @Nullable String env,
      @JsonProperty @Nullable String webRoot,
      @JsonProperty @Nullable String jwtKeyName,
      @JsonProperty @Nullable String jwtPubKey,
      @JsonProperty @Nullable Boolean debug
  ) {
    if (serverName == null || serverName.length() == 0) {
      serverName = "Naksha";
    }
    if (hostId == null || hostId.length() == 0) {
      hostId = UUID.randomUUID().toString();
    }
    if (db == null) {
      db = new PsqlConfigBuilder()
          .withHost("localhost")
          .withPort(5432)
          .withDb("postgres")
          .withUser("postgres")
          .withPassword("password")
          .withAppName(APP_NAME)
          .withSchema(APP_NAME)
          .build();
    }
    if (httpPort != null && (httpPort < 0 || httpPort > 65535)) {
      currentLogger().error("Invalid port in Naksha configuration: " + httpPort);
      httpPort = 7080;
    } else if (httpPort == null || httpPort == 0) {
      httpPort = 7080;
    }
    if (hostname == null || hostname.length() == 0) {
      try {
        hostname = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        currentLogger().error("Unable to resolve the hostname using Java's API.", e);
        hostname = "localhost";
      }
    }
    URL __endpoint = null;
    if (endpoint != null && endpoint.length() > 0) {
      try {
        __endpoint = new URL(endpoint);
      } catch (MalformedURLException e) {
        currentLogger().error("Invalid configuration of endpoint: {}", endpoint, e);
      }
    }
    if (__endpoint == null) {
      try {
        __endpoint = new URL("http://" + hostname + ":" + httpPort);
      } catch (MalformedURLException e) {
        currentLogger().error("Invalid hostname: {}", hostname, e);
        hostname = "localhost";
        try {
          __endpoint = new URL("http://localhost:" + httpPort);
        } catch (MalformedURLException ignore) {
        }
      }
      assert __endpoint != null;
    }
    if (env == null) {
      env = "local";
    }

    this.serverName = serverName;
    this.hostId = hostId;
    this.db = db;
    this.httpPort = httpPort;
    this.hostname = hostname;
    this.endpoint = __endpoint;
    this.env = env;
    this.webRoot = webRoot;
    this.jwtKeyName = jwtKeyName != null && !jwtKeyName.isEmpty() ? jwtKeyName : "jwt";
    this.jwtPubKey = jwtPubKey;
    this.debug = Boolean.TRUE.equals(debug);
  }

  /**
   * The server name to use.
   */
  public final @NotNull String serverName;

  /**
   * The unique host identifier.
   */
  public final @NotNull String hostId;

  /**
   * The configuration for the Naksha admin database.
   */
  public final @NotNull PsqlConfig db;

  /**
   * The port at which to listen for HTTP requests.
   */
  public final int httpPort;

  /**
   * The hostname to use to refer to this instance, if {@code null}, then auto-detected.
   */
  public final @NotNull String hostname;

  /**
   * The public endpoint, for example "https://naksha.foo.com/". If {@code null}, then the hostname and HTTP port used.
   */
  public @NotNull URL endpoint;

  /**
   * The environment, for example "local", "dev", "e2e" or "prd".
   */
  public final @NotNull String env;

  /**
   * If set, then serving static files from this directory.
   */
  public final @Nullable String webRoot;

  /**
   * If the JWT key should be read from the disk ({@code "~/.config/naksha/auth/$<jwtKeyName>.pub"}), evaluated after {@link #jwtPubKey}.
   */
  public final @NotNull String jwtKeyName;

  /**
   * If the public key given via configuration, rather than as file in the {@code "~/.config/naksha/auth/$<jwtKeyName>.pub"}) directory. If
   * this is given, the service is unable to run tests, because it can't generate own tokens!
   */
  public final @Nullable String jwtPubKey;

  /**
   * If debugging mode is enabled.
   */
  public boolean debug;
}
