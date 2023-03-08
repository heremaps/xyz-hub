package com.here.xyz.pub.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PubConfig {

  // Existing XYZ configuration
  protected String DEFAULT_STORAGE_ID;

  public final @NotNull String DEFAULT_STORAGE_ID() {
    if (DEFAULT_STORAGE_ID == null) {
      throw new Error("DEFAULT_STORAGE_ID must not be null");
    }
    return DEFAULT_STORAGE_ID;
  }

  protected String STORAGE_DB_URL;

  public final @NotNull String STORAGE_DB_URL() {
    if (STORAGE_DB_URL == null) {
      throw new Error("STORAGE_DB_URL must not be null");
    }
    return STORAGE_DB_URL;
  }

  protected String STORAGE_DB_USER;

  public final @NotNull String STORAGE_DB_USER() {
    if (STORAGE_DB_USER == null) {
      throw new Error("STORAGE_DB_USER must not be null");
    }
    return STORAGE_DB_USER;
  }

  protected String STORAGE_DB_PASSWORD;

  public final @NotNull String STORAGE_DB_PASSWORD() {
    if (STORAGE_DB_PASSWORD == null) {
      throw new Error("STORAGE_DB_PASSWORD must not be null");
    }
    return STORAGE_DB_PASSWORD;
  }

  // Transaction Publisher specific configuration
  public boolean ENABLE_TXN_PUBLISHER = true;
  public long TXN_PUB_JOB_INITIAL_DELAY_MS = TimeUnit.SECONDS.toMillis(15);
  public long TXN_PUB_JOB_SUBSEQUENT_DELAY_MS = TimeUnit.SECONDS.toMillis(2);
  // One thread per subscription
  public int TXN_PUB_TPOOL_CORE_SIZE = 1;
  public int TXN_PUB_TPOOL_MAX_SIZE = 100;
  public long TXN_PUB_TPOOL_KEEP_ALIVE_SEC = TimeUnit.MINUTES.toSeconds(1);
  // AWS connection
  public @Nullable String AWS_ACCESS_KEY_ID;
  public @Nullable String AWS_SECRET_ACCESS_KEY;
  public @NotNull String AWS_DEFAULT_REGION = "us-east-1"; // default region

  // Transaction Sequencer specific configuration
  public boolean ENABLE_TXN_SEQUENCER = true;
  public long TXN_SEQ_JOB_INITIAL_DELAY_MS = TimeUnit.SECONDS.toMillis(15);
  public long TXN_SEQ_JOB_SUBSEQUENT_DELAY_MS = TimeUnit.SECONDS.toMillis(2);
  // One thread per connector (i.e. per SpaceDB)
  public int TXN_SEQ_TPOOL_CORE_SIZE = 1;
  public int TXN_SEQ_TPOOL_MAX_SIZE = 10;
  public long TXN_SEQ_TPOOL_KEEP_ALIVE_SEC = TimeUnit.MINUTES.toSeconds(1);

  public static final String XYZ_ADMIN_DB_CFG_SCHEMA = "xyz_config"; // default config schema
}
