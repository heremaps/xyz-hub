package com.here.xyz.hub;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.here.mapcreator.ext.naksha.PsqlPoolConfig;
import com.here.mapcreator.ext.naksha.PsqlPoolConfigBuilder;
import com.here.xyz.util.JsonConfigFile;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The XYZ-Hub service configuration.
 */
@SuppressWarnings("UnusedReturnValue")
public class NakshaHubConfig extends JsonConfigFile<NakshaHubConfig> {

  static final Logger logger = LoggerFactory.getLogger(NakshaHubConfig.class);

  /**
   * The default name of the configuration file.
   */
  public static final String DEFAULT_CONFIG = "config.json";

  public NakshaHubConfig() {
    super(DEFAULT_CONFIG);
  }

  public NakshaHubConfig(@NotNull String filename) {
    super(filename);
  }

  /**
   * The server name to use.
   */
  private String serverName;

  @JsonGetter
  public @NotNull String getServerName() {
    return serverName != null ? serverName : "Naksha";
  }

  @JsonSetter
  public void setServerName(@NotNull String name) {
    this.serverName = name;
  }

  /**
   * The configuration for the Naksha admin database.
   */
  private PsqlPoolConfig db;

  @JsonGetter
  public @NotNull PsqlPoolConfig getDb() {
    if (this.db == null) {
      this.db = new PsqlPoolConfigBuilder()
          .withDb("postgres")
          .withUser("postgres")
          .withPassword("password")
          .withHost("localhost")
          .withPort(5432)
          .build();
    }
    return this.db;
  }

  @JsonSetter
  public void setDb(@NotNull PsqlPoolConfig db) {
    this.db = db;
  }

  @JsonGetter
  public int getHttpPort() {
    return httpPort > 0 && httpPort <= 65535 ? httpPort : 7080;
  }

  @JsonSetter
  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  /**
   * The port at which to listen for HTTP requests.
   */
  @JsonInclude(Include.NON_DEFAULT)
  private int httpPort = 7080;

  // TODO: Implement HTTPS
  /**
   * The port at which to listen for HTTPS requests.
   */
  //public int httpsPort = 7443;

  public @NotNull String getHostname() {
    if (hostname != null && hostname.length() > 0) {
      return hostname;
    }
    try {
      hostname = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      logger.error("Unable to resolve the hostname using Java's API.", e);
      hostname = "localhost";
    }
    return hostname;
  }

  public void setHostname(@Nullable String hostname) {
    this.hostname = hostname;
  }

  /**
   * The hostname to use to refer to this instance, if {@code null}, then auto-detected.
   */
  @JsonInclude(Include.NON_EMPTY)
  private String hostname;

  @JsonIgnore
  public @NotNull URL getEndpoint() {
    if (__endpoint != null) {
      return __endpoint;
    }
    if (endpoint != null && endpoint.length() > 0) {
      try {
        __endpoint = new URL(endpoint);
      } catch (MalformedURLException e) {
        logger.error("Invalid configuration of endpoint: {}", endpoint, e);
      }
    }
    if (__endpoint == null) {
      try {
        __endpoint = new URL("http://" + getHostname() + ":" + getHttpPort());
      } catch (MalformedURLException e) {
        logger.error("Invalid hostname: {}", getHostname(), e);
        try {
          __endpoint = new URL("http://localhost:" + getHttpPort());
        } catch (MalformedURLException ignore) {
          // Note: Will not happen
        }
      }
    }
    assert __endpoint != null;
    return __endpoint;
  }

  @JsonIgnore
  public void setEndpoint(@Nullable String endpoint) {
    this.endpoint = endpoint;
    this.__endpoint = null;
  }

  /**
   * The public endpoint, for example "https://naksha.foo.com/". If {@code null}, then the hostname and HTTP port used.
   */
  @JsonProperty
  @JsonInclude(Include.NON_EMPTY)
  private String endpoint;

  @JsonIgnore
  private URL __endpoint;

  /**
   * The environment, for example "local", "dev", "e2e" or "prd".
   */
  @JsonInclude(Include.NON_DEFAULT)
  private String env = "local";

  @JsonGetter
  public @NotNull String getEnv() {
    return env != null ? env : "local";
  }

  @JsonSetter
  public void setEnv(@NotNull String env) {
    this.env = env;
  }

  @JsonGetter
  public @Nullable String getWebRoot() {
    return webRoot;
  }

  @JsonSetter
  public void setWebRoot(@Nullable String webRoot) {
    this.webRoot = webRoot;
  }

  /**
   * If set, then serving static files from this directory.
   */
  @JsonInclude(Include.NON_EMPTY)
  private String webRoot;

  /**
   * If the JWT key should be read from the disk ({@code "~/.config/naksha/auth/$<jwtKeyName>.pub"}), evaluated after {@link #jwtPubKey}.
   */
  private String jwtKeyName;

  @JsonSetter
  public void setJwtKeyName(@NotNull String keyName) {
    this.jwtKeyName = keyName;
  }

  @JsonGetter
  public @NotNull String getJwtKeyName() {
    return jwtKeyName != null && jwtKeyName.length() > 0 ? jwtKeyName : "jwt";
  }

  @JsonGetter
  public @Nullable String getJwtPubKey() {
    return jwtPubKey;
  }

  @JsonSetter
  public void setJwtPubKey(@Nullable String jwtPubKey) {
    this.jwtPubKey = jwtPubKey;
  }

  /**
   * If the public key given via configuration, rather than as file in the {@code "~/.config/naksha/auth/$<jwtKeyName>.pub"}) directory. If
   * this is given, the service is unable to run tests, because it can't generate own tokens!
   */
  private String jwtPubKey;

  /**
   * If debugging mode is enabled.
   */
  @JsonInclude(Include.NON_DEFAULT)
  public boolean debug = false;

  // ----------------------------------------------------------------------------------------------------------------------------------

  @Override
  protected @NotNull String envPrefix() {
    return "NAKSHA_";
  }

  @Override
  protected @NotNull String appName() {
    return "naksha";
  }
}
