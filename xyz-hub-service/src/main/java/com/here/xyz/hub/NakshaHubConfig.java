package com.here.xyz.hub;

import com.here.xyz.util.JsonConfigFile;
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

  // ----------------------------------------------------------------------------------------------------------------------------------
  // -------------------< CONFIG >-----------------------------------------------------------------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------------------------

  /**
   * The port at which to listen for HTTP requests.
   */
  public int httpPort = 7080;

  // TODO: Implement HTTPS
  /**
   * The port at which to listen for HTTPS requests.
   */
  //public int httpsPort = 7443;

  /**
   * The hostname to use to refer to this instance, if {@code null}, then auto-detected.
   */
  public String hostname;

  /**
   * The public endpoint, for example "https://naksha.foo.com/". If {@code null}, then the hostname and HTTP port used.
   */
  public String endpoint;

  /**
   * The environment, for example "local", "dev", "e2e" or "prd".
   */
  public String env = "local";

  /**
   * If the JWT key should be read from the disk ({@code "~/.config/naksha/auth/$<jwtKeyName>.pub"}), evaluated after {@link #jwtPubKey}.
   */
  public String jwtKeyName;

  public @NotNull String jwtKeyName() {
    return jwtKeyName != null && jwtKeyName.length() > 0 ? jwtKeyName : "jwt";
  }

  /**
   * If the public key given via configuration, rather than as file in the {@code "~/.config/naksha/auth/$<jwtKeyName>.pub"}) directory.
   * If this is given, the service is unable to run tests, because it can't generate own tokens!
   */
  public String jwtPubKey;

  /**
   * If debugging mode is enabled.
   */
  public boolean debug = false;

  public @NotNull NakshaHubConfig enableDebug() {
    this.debug = true;
    return this;
  }

  // ----------------------------------------------------------------------------------------------------------------------------------

  @Override
  protected @NotNull String envPrefix() {
    return "NAKSHA_";
  }

  @Override
  protected @NotNull String appName() {
    return "naksha";
  }

  protected void info(@NotNull String message) {
    logger.info(message);
  }

  protected void error(@NotNull String message, @Nullable Throwable t) {
    logger.error(message, t);
  }
}
