package com.here.xyz.hub;

import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * That message can be used to change the log-level of one or more service-nodes. The specified level must be a valid log-level. As this is
 * a {@link RelayedMessage} it can be sent to a specific service-node or to all service-nodes regardless of the first service node by which
 * it was received.
 *
 * Specifying the property {@link RelayedMessage#relay} to true will relay the message to the specified destination. If no destination is
 * specified the message will be relayed to all service-nodes (broadcast).
 */
@SuppressWarnings("unused")
class ChangeLogLevelMessage extends RelayedMessage {

  private static final Logger logger = LogManager.getLogger();

  private String level;

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  @Override
  protected void handleAtDestination() {
    logger.info("LOG LEVEL UPDATE requested. New level will be: " + level);
    Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.getLevel(level));
    logger.info("LOG LEVEL UPDATE performed. New level is now: " + level);
  }
}
